package com.lee.distributedlock.util.redis.annotation;

import java.lang.annotation.*;

/**
 * 锁的参数，需要配合 @CacheLock 使用
 *  key 的生成规则是由自己定义的 CacheKeyGenerator 实现类实现
 */
@Target({ElementType.PARAMETER, ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface CacheParam {

    /**
     * 字段名称
     *
     * @return String
     */
    String name() default "";
}