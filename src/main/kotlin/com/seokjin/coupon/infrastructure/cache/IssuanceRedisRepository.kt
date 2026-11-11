package com.seokjin.coupon.infrastructure.cache

import com.seokjin.coupon.infrastructure.cache.RedisKeys
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Repository

@Repository
class IssuanceRedisRepository(
    private val redis: StringRedisTemplate,
    private val soldOutProperties: SoldOutProperties
) {
    private val issueScript = longLuaScript("lua/issue.lua")


    //반환 1= 성공, 0 =매진, -1= 중복발급
    fun tryIssue(couponId: Long, userId: Long): Long =
        redis.runForLong(
            issueScript,
            listOf(
                RedisKeys.couponStock(couponId),
                RedisKeys.couponUsers(couponId),
                RedisKeys.couponSoldOut(couponId),
                RedisKeys.reconcileKey
            ),
            userId, soldOutProperties.ttlSeconds, System.currentTimeMillis(), couponId
        )

    fun initStock(couponId: Long, totalQuantity: Int) {
        redis.opsForValue().set(RedisKeys.couponStock(couponId), totalQuantity.toString())
    }

}