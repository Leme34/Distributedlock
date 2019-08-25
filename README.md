对比：
1、Redis 的读写性能肯定比 zookeeper 高。zookeeper 可靠性高。
2、zookeeper 的 watch 机制，可以通知到下一个操作共享资源的客户端。这样客户端可以一直等待，直到收到通知再去获取资源锁。
而 Redis 没有通知机制，所以 Redis 的分布式锁适合用于防止设定时间内的重复提交。