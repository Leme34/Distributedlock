package com.lee.distributedlock.util.redis.aspect;

import com.lee.distributedlock.util.redis.RedisLockHelper;
import com.lee.distributedlock.util.redis.annotation.CacheLock;
import com.lee.distributedlock.util.redis.keyGenerator.CacheKeyGenerator;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.UUID;

@Aspect
@Configuration
public class LockMethodInterceptor {

    @Autowired
    private RedisLockHelper redisLockHelper;
    @Qualifier("lockKeyGenerator")
    private CacheKeyGenerator cacheKeyGenerator;


    @Around("execution(public * *(..)) && @annotation(com.lee.distributedlock.util.redis.annotation.CacheLock)")
    public Object interceptor(ProceedingJoinPoint pjp) {
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        Method method = signature.getMethod();
        CacheLock lock = method.getAnnotation(CacheLock.class);
        if (StringUtils.isEmpty(lock.prefix())) {
            throw new RuntimeException("lock key don't null...");
        }
        // 分布式锁的key
        final String lockKey = cacheKeyGenerator.getLockKey(pjp);
        // client(最好是唯一键的)，分布式锁的值= 时间戳 + "|" +uuid
        String uuid = UUID.randomUUID().toString();
        try {
            // 假设上锁成功，但是设置过期时间失效，以后拿到的都是 false
            final boolean success = redisLockHelper.lock(lockKey, uuid, lock.expire());
            if (!success) {
                throw new RuntimeException("重复提交");
            }
            try {
                // 执行业务逻辑
                return pjp.proceed();
            } catch (Throwable throwable) {
                throw new RuntimeException("系统异常");
            }
        } finally {
            // 执行完后解锁
            redisLockHelper.unlock(lockKey, uuid);
        }
    }
}
