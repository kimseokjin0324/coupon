package com.seokjin.coupon.infrastructure.cache

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Repository
import java.util.concurrent.TimeUnit

@Repository
class CouponReconcileRedisRepository(
    private val redisTemplate: StringRedisTemplate,
) {

    fun couponIdsIssuedBetween(fromExclusiveMs: Long, toInclusiveMs: Long): List<Long> {
        if (toInclusiveMs <= fromExclusiveMs) return emptyList()
        val members = redisTemplate.opsForZSet().rangeByScore(
            RedisKeys.reconcileKey,
            (fromExclusiveMs + 1).toDouble(),
            toInclusiveMs.toDouble()
        ) ?: emptySet()

        return members.mapNotNull { it.toLongOrNull() }

    }

    fun stock(couponId: Long): Long? =
        redisTemplate.opsForValue().get(RedisKeys.couponStock(couponId))?.toLongOrNull()

    fun userCount(couponId: Long): Long =
        redisTemplate.opsForSet().size(RedisKeys.couponUsers(couponId)) ?: 0L

    fun userIds(couponId: Long): Set<String> =
        redisTemplate.opsForSet().members(RedisKeys.couponUsers(couponId)) ?: emptySet()

    fun soldOutExists(couponId: Long): Boolean =
        redisTemplate.hasKey(RedisKeys.couponSoldOut(couponId))

    //대사에서 사용자수가 맞지않으면 자동으로 유저를 추가할때
    fun addUsers(couponId: Long, userIds: Collection<Long>) {
        if (userIds.isEmpty()) return
        redisTemplate.opsForSet().add(RedisKeys.couponUsers(couponId), *userIds.map { it.toString() }.toTypedArray())
    }

    //매진 플레그가 잘못 설정된경우 삭제하는 메소드
    fun deleteSoldOut(couponId: Long) {
        redisTemplate.delete(RedisKeys.couponSoldOut(couponId))
    }

    //마지막으로 만약에 솔드아웃이 없을 때 우리가 알고보니 매진이었다면 매진으로 세팅
    fun setSoldOut(couponId: Long, ttlSeconds: Long) {
        redisTemplate.opsForValue().set(RedisKeys.couponSoldOut(couponId), "1", ttlSeconds, TimeUnit.SECONDS)
    }
}