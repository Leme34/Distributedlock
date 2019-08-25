package com.lee.distributedlock.util.redis.controller;

import com.lee.distributedlock.util.redis.annotation.CacheLock;
import com.lee.distributedlock.util.redis.annotation.CacheParam;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/books")
public class BookController {

    /**
     * 若该接口 token = 1，那么最终缓存的 key 是 "books:1"，如果多个条件则依次类推
     * @param token
     * @return
     */
    @CacheLock(prefix = "books")
    @GetMapping
    public String query(@CacheParam(name = "token") @RequestParam String token) {
        return "success - " + token;
    }

}