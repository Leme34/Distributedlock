package com.lee.distributedlock.util.redis.keyGenerator;

import com.lee.distributedlock.util.redis.annotation.CacheLock;
import com.lee.distributedlock.util.redis.annotation.CacheParam;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * Key 生成策略实现类
 * 主要是解析被 @CacheLock 注解的方法的信息，获取对应的属性值，生成一个全新的缓存 Key
 */
@Component("lockKeyGenerator")
public class LockKeyGenerator implements CacheKeyGenerator {

    @Override
    public String getLockKey(ProceedingJoinPoint pjp) {
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        Method method = signature.getMethod();
        CacheLock lockAnnotation = method.getAnnotation(CacheLock.class);
        // 被@CacheLock注解的方法的所有参数对象
        final Object[] args = pjp.getArgs();
        // 被@CacheLock注解的方法的所有参数信息（包括参数名）
        final Parameter[] parameters = method.getParameters();
        StringBuilder builder = new StringBuilder();
        // TODO 默认解析方法里面带 CacheParam 注解的属性,如果没有尝试着解析实体对象中的
        for (int i = 0; i < parameters.length; i++) {
            final CacheParam annotation = parameters[i].getAnnotation(CacheParam.class);
            // 第i个参数没有标注 @CacheParam，遍历下一个参数
            if (annotation == null) {
                continue;
            }
            // 拼接上":"+第i个参数值
            builder.append(lockAnnotation.delimiter()).append(args[i]);
        }
        if (StringUtils.isEmpty(builder.toString())) {
            // 获取该方法上的所有参数注解信息
            final Annotation[][] parameterAnnotations = method.getParameterAnnotations();
            for (int i = 0; i < parameterAnnotations.length; i++) {
                // 遍历第i个方法参数看作实体对象解析，遍历其所有属性，找出标了 @CacheParam 的属性
                final Object object = args[i];
                final Field[] fields = object.getClass().getDeclaredFields();
                for (Field field : fields) {
                    final CacheParam annotation = field.getAnnotation(CacheParam.class);
                    if (annotation == null) {
                        continue;
                    }
                    field.setAccessible(true);
                    // 拼接上":"+被注解的这个属性值
                    builder.append(lockAnnotation.delimiter()).append(ReflectionUtils.getField(field, object));
                }
            }
        }
        return lockAnnotation.prefix() + builder.toString();
    }
}