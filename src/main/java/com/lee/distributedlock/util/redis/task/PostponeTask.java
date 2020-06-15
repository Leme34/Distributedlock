package com.lee.distributedlock.util.redis.task;

import com.lee.distributedlock.util.redis.RedisLockHelper;

public class PostponeTask implements Runnable {

    private String key;
    private String value;
    private long expireTime;
    private boolean isRunning;
    private RedisLockHelper redisLockHelper;

    public PostponeTask(String key, String value, long expireTime, RedisLockHelper redisLockHelper) {
        this.key = key;
        this.value = value;
        this.expireTime = expireTime;
        this.isRunning = Boolean.TRUE;
        this.redisLockHelper = redisLockHelper;
    }

    @Override
    public void run() {
        long waitTime = expireTime * 1000 * 2 / 3;// 线程等待多长时间后执行
        while (isRunning) {
            try {
                Thread.sleep(waitTime);
                if (redisLockHelper.postpone(key, value, expireTime)) {
                    System.out.println("锁延时成功...........................................................");
                } else {
                    this.stop();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void stop() {
        this.isRunning = Boolean.FALSE;
    }

}
