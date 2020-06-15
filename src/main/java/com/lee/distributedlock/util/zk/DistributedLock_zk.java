package com.lee.distributedlock.util.zk;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

/*
    使用方法：

    //注入zk分布式锁工具类，调用初始化方法init(),监听zk节点的事件
    @Bean(initMethod="init")
    public DistributedLock distributedLock() {
        return new DistributedLock();
    }
 */

/**
 * zookeeper分布式锁工具类
 */
@Slf4j
@AllArgsConstructor
@Component
public class DistributedLock_zk {

    @Value("${ZOOKEEPER_SERVER}")
    private String ZOOKEEPER_SERVER;   //zookeeper所在url

    // 操作zk的客户端对象，由init()初始化
    private CuratorFramework client;

    // juc的计数器,调用其await()阻塞并挂起请求，等待上一个锁持有者调用countDown()使count=0（释放锁）才继续执行
    private static CountDownLatch zkLockLatch = new CountDownLatch(1);

    // 分布式锁的总节点名称，不同项目中不同
    private static final String ZK_LOCK_PROJECT = "demo-locks";
    // 分布式锁节点名称
    private static final String DISTRIBUTED_LOCK = "distributed_lock";

    /**
     * 初始化锁
     * 锁的层次结构：
     * ZKLocks-NameSpace(命名空间)
     * |—— demo-locks(总节点名称)
     * |—— distributed_lock(锁节点名称)
     */
    public void init() {
        // 创建zk客户端
        CuratorFrameworkFactory.builder()
                .connectString(ZOOKEEPER_SERVER)   //zk所在url
                .sessionTimeoutMs(10000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))    //重试策略
                .namespace("ZKLocks-NameSpace")  //命名空间
                .build();
        // 启动客户端
        client.start();
        try {
            // 创建总节点
            if (client.checkExists().forPath("/" + ZK_LOCK_PROJECT) == null) {
                client.create()
                        .creatingParentsIfNeeded()                //递归创建
                        .withMode(CreateMode.PERSISTENT)          //父节点使用永久性节点
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)     //默认acl权限到
                        .forPath("/" + ZK_LOCK_PROJECT);
            }
            // 监听锁的父节点下所有子节点的事件
            addWatcherToLock("/" + ZK_LOCK_PROJECT);
        } catch (Exception e) {
            log.error("客户端连接zk失败...", e);
        }
    }

    /**
     * 尝试获取分布式锁，获取成功则return，获取失败则阻塞并挂起当前线程
     */
    public void getLock() {
        // 死循环，当且仅当上一个锁释放、当前请求获取锁成功时才会跳出
        while (true) {
            try {
                client.create()
                        .creatingParentsIfNeeded()                //递归创建
                        .withMode(CreateMode.EPHEMERAL)           //子节点必须使用临时节点
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)     //默认acl权限到
                        .forPath("/" + ZK_LOCK_PROJECT + "/" + DISTRIBUTED_LOCK);
                log.info("获取分布式锁成功...");
                return;
            } catch (Exception e) {
                // 锁节点已存在获取锁失败
                log.info("获取分布式锁失败...");
                try {
                    // 若锁已被释放过，重置同步资源的值并挂起线程
                    if (zkLockLatch.getCount() <= 0) {
                        zkLockLatch = new CountDownLatch(1);
                    }
                    // 调用await()方法的线程（主线程）会被挂起，它会等待直到count值为0才继续执行
                    zkLockLatch.await();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }


    /**
     * 删除锁节点，会触发zk监听器修改计数器进而释放分布式锁
     *
     * @return 释放成功返回true，否则返回false
     */
    public boolean releaseLock() {
        try {
            // 删除锁节点
            String lockPath = "/" + ZK_LOCK_PROJECT + "/" + DISTRIBUTED_LOCK;
            if (client.checkExists().forPath(lockPath) != null) {
                client.delete().forPath(lockPath);
            }
        } catch (Exception e) {
            log.info("分布式锁释放失败...");
            e.printStackTrace();
            return false;
        }
        log.info("分布式锁释放完成...");
        return true;
    }


    /**
     * 监听path节点
     */
    public void addWatcherToLock(String path) throws Exception {
        // 缓存此节点下所有子节点的状态信息
        final PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        // 添加回调监听器
        cache.getListenable().addListener((client, event) -> {
            // 监听删除子节点事件（释放锁）
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                String p = event.getData().getPath();
                // 若是分布式锁节点
                if (p.contains(DISTRIBUTED_LOCK)) {
                    // 使count=0，释放锁
                    zkLockLatch.countDown();
                    log.info("已释放同步计数器，让当前请求获得分布式锁...");
                }
                log.info("上一个会话已释放锁或已断开，锁节点路径：" + p);
            }
        });
    }
}
