package com.seokjin.coupon.application

import com.seokjin.coupon.infrastructure.cache.IssuanceRedisRepository
import com.seokjin.coupon.support.AlreadyIssuedException
import com.seokjin.coupon.support.SoldOutException
import org.springframework.core.io.ClassPathResource
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.RedisScript
import org.springframework.stereotype.Component

@Component
class CouponIssuer(
    private val issuanceRedisRepository: IssuanceRedisRepository
) {

    fun tryIssue(couponId: Long, userId: Long) {

        when (issuanceRedisRepository.tryIssue(couponId, userId)) {
            1L -> Unit
            0L -> throw SoldOutException()
            -1L -> throw AlreadyIssuedException()
            else -> error("예상치 못한 Lua 결과")
        }
    }

    fun initStock(couponId: Long, totalQuantity: Int) {
        issuanceRedisRepository.initStock(couponId, totalQuantity)
    }

}