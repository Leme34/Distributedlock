package com.lee.distributedlock.util.redis;

import com.google.common.collect.Lists;
import com.lee.distributedlock.util.redis.task.PostponeTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Configuration
@AutoConfigureAfter(RedisAutoConfiguration.class)
public class RedisLockHelper {

    private static final String DELIMITER = "|";

    private static final Long RELEASE_SUCCESS = 1L;
    private static final Long POSTPONE_SUCCESS = 1L;

    private static final String LOCK_SUCCESS = "OK";
    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE_TIME = "EX";
    // 解锁脚本(lua)
    private static final String RELEASE_LOCK_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
    // 延时脚本
    private static final String POSTPONE_LOCK_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('expire', KEYS[1], ARGV[2]) else return '0' end";

    /**
     * 如果要求比较高可以通过注入的方式分配
     */
    private static final ScheduledExecutorService EXECUTOR_SERVICE =
            Executors.newScheduledThreadPool(10);

    @Autowired
    private StringRedisTemplate redisTemplate;

    /**
     * 获取锁（存在死锁风险）
     * 在使用 SETNX 获得锁时，我们将键 lock.id 的值设置为锁的有效时间，线程获得锁后，其他线程还会不断的检测锁是否已超时，
     * 如果超时，等待的线程也将有机会获得锁。然而，锁超时，我们不能简单地使用 DEL 命令删除键 lock.id 以释放锁。
     * <p>
     * 场景：
     * 1、A已经首先获得了锁 lock.id，然后线A断线。B,C都在等待竞争该锁；
     * 2、B,C读取lock.id的值，比较当前时间和键 lock.id 的值来判断是否超时，发现超时；
     * 3、B执行 DEL lock.id命令，并执行 SETNX lock.id 命令，并返回1，B获得锁；
     * 4、C由于各刚刚检测到锁已超时，执行 DEL lock.id命令，将B刚刚设置的键 lock.id 删除，执行 SETNX lock.id命令，并返回1，即C获得锁。
     * <p>
     * 上面的步骤很明显出现了问题，导致B,C同时获取了锁。在检测到锁超时后，线程不能直接简单地执行 DEL 删除键的操作以获得锁。
     *
     * @param lockKey lockKey
     * @param value   value
     * @param time    超时时间
     * @param unit    过期单位
     * @return true or false
     */
    public boolean tryLock(final String lockKey, final String value, final long time, final TimeUnit unit) {
        return redisTemplate.execute(
                (RedisCallback<Boolean>) connection ->
                        connection.set(lockKey.getBytes(), value.getBytes(), Expiration.from(time, unit),
                                RedisStringCommands.SetOption.SET_IF_ABSENT)
        );
    }

    /**
     * 获取锁
     * 缺点：
     * 1.没有解决当锁已超时而业务逻辑还未执行完的问题（例如：A线程超时时间设为10s(为了解决死锁问题), 但代码执行时间可能需要30s, 然后redis服务端10s后将锁删除, 此时, B线程恰好申请锁, redis服务端不存在该锁, 可以申请, 也执行了代码, 那么问题来了, A、B线程都同时获取到锁并执行业务逻辑, 这与分布式锁最基本的性质相违背: 在任意一个时刻, 只有一个客户端持有锁, 即独享）
     * 2.加锁与设置超时并非原子操作，有死锁风险，假设上锁成功，但是设置过期时间失败，以后拿到的都是 false
     *
     * <p>
     * 在释放锁，即执行 DEL lock.id 操作前，需要先判断锁是否已超时。
     * 如果锁已超时，那么锁可能已由其他线程获得，这时直接执行 DEL lock.id 操作会导致把其他线程已获得的锁释放掉。
     * 问题是出在删除键的操作上面，那么获取锁之后应该怎么改进呢？
     * 解决办法：GETSET key value，将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
     * <p>
     * 场景：
     * 1、A已经首先获得了锁 lock.id，然后线A断线。B,C都在等待竞争该锁；
     * 2、B,C读取lock.id的值，比较当前时间和键 lock.id 的值来判断是否超时，发现超时；
     * 3、B检测到锁已超时，即当前的时间大于键 lock.id 的值，B会执行
     * 4、GETSET lock.id <current Unix timestamp + lock timeout + 1>设置时间戳，通过比较键 lock.id 的旧值是否小于当前时间，判断进程是否已获得锁；
     * 5、B发现GETSET返回的值小于当前时间，则执行 DEL lock.id命令，并执行 SETNX lock.id 命令，并返回1，B获得锁；
     * 6、C执行GETSET得到的时间大于当前时间，则继续等待。
     *
     * @param lockKey lockKey
     * @param uuid    UUID
     * @param timeout 超时时间
     * @param unit    过期单位
     * @return true or false
     */
    public boolean lockNotSafely(String lockKey, final String uuid, long timeout, final TimeUnit unit) {
        // 根据过期时间的单位，换算成毫秒
        final long milliseconds = Expiration.from(timeout, unit).getExpirationTimeInMilliseconds();
        // 如果缓存中没有当前 key 则进行缓存同时返回 true
        boolean success = redisTemplate.opsForValue()
                .setIfAbsent(lockKey, (System.currentTimeMillis() + milliseconds) + DELIMITER + uuid);
        if (success) {   // 以前未持有锁，加锁成功，设置过期时间
            redisTemplate.expire(lockKey, timeout, TimeUnit.SECONDS);
            return true;
        } else {         // 以前已持有锁
            // 获取上一次的锁过期时间，并设置现在锁的过期时间
            String oldVal = redisTemplate.opsForValue()
                    .getAndSet(lockKey, (System.currentTimeMillis() + milliseconds) + DELIMITER + uuid);
            final String[] oldValues = oldVal.split(Pattern.quote(DELIMITER));
            // 若上一次的锁过期时间 + 1ms <= 当前时间（锁已过期），获取锁成功，返回true，否则加锁失败false
            return Long.parseLong(oldValues[0]) + 1 <= System.currentTimeMillis();
        }
    }


    /**
     * 调用lock同时, 立即开启PostponeTask线程, 线程等待超时时间的2/3时间后, 开始执行锁延时代码,
     * 如果延时成功, add_information_lock这个key会一直存在于redis服务端, 直到业务逻辑执行完毕,
     * 因此在此过程中, 其他线程无法获取到锁, 也即保证了线程安全性
     *
     * 解决了锁超时而业务逻辑仍在执行的锁冲突问题, 还很简陋, 而最严谨的方式还是使用官方的 Redlock 算法实现,
     * 其中 Java 包推荐使用 redisson, 思路差不多其实, 都是在快要超时时续期, 以保证业务逻辑未执行完毕不会有其他客户端持有锁
     *
     * @param lockKey lockKey
     * @param uuid    UUID
     * @param timeout 超时时间，单位: 秒
     * @return true or false
     */
    public boolean lock(String lockKey, final String uuid, long timeout) {
        // 加锁
        Boolean locked = redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
            Jedis jedis = (Jedis) redisConnection.getNativeConnection();
            String result = jedis.set(lockKey, uuid, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, timeout);
            if (LOCK_SUCCESS.equals(result)) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        });

        if (locked) {
            // 加锁成功, 启动一个延时线程, 防止业务逻辑未执行完毕就因锁超时而使锁释放
            PostponeTask postponeTask = new PostponeTask(lockKey, uuid, timeout, this);
            Thread thread = new Thread(postponeTask);
            thread.setDaemon(Boolean.TRUE);
            thread.start();
        }

        return locked;
    }


    /**
     * 解锁
     *
     * @param lockKey key
     * @param uuid    client(最好是唯一键的)
     */
    public void unlock(String lockKey, String uuid) {
        this.unlock(lockKey, uuid, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * 延迟解锁
     *
     * @param lockKey   key
     * @param uuid      client(最好是唯一键的)
     * @param delayTime 延迟时间
     * @param unit      时间单位
     */
    public void unlock(final String lockKey, final String uuid, long delayTime, TimeUnit unit) {
        if (StringUtils.isEmpty(lockKey)) {
            return;
        }
        if (delayTime <= 0) {
            this.doUnlock(lockKey, uuid);
        } else {
            EXECUTOR_SERVICE.schedule(() -> this.doUnlock(lockKey, uuid), delayTime, unit);
        }
    }

    /**
     * 解除该 uuid 对应的客户端加的锁
     *
     * @param lockKey key
     * @param uuid    client(最好是唯一键的)
     */
    private void doUnlock(final String lockKey, final String uuid) {
        String val = redisTemplate.opsForValue().get(lockKey);
        final String[] values = val.split(Pattern.quote(DELIMITER));
        if (values.length <= 0) {
            return;
        }
        if (uuid.equals(values[1])) {
            redisTemplate.delete(lockKey);
        }
    }

    /**
     * 锁延时
     *
     * @param key
     * @param value
     * @param expireTime
     * @return
     */
    public Boolean postpone(String key, String value, long expireTime) {
        return redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
            Jedis jedis = (Jedis) redisConnection.getNativeConnection();
            Object result = jedis.eval(POSTPONE_LOCK_SCRIPT, Lists.newArrayList(key),
                    Lists.newArrayList(value, String.valueOf(expireTime)));
            if (POSTPONE_SUCCESS.equals(result)) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        });
    }

}
