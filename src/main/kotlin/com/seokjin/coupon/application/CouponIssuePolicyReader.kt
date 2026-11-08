package com.seokjin.coupon.application

import com.seokjin.coupon.domain.CouponRepository
import com.seokjin.coupon.infrastructure.cache.CacheProperties
import com.seokjin.coupon.infrastructure.cache.CouponCacheRepository
import com.seokjin.coupon.support.CouponNotFoundException
import org.springframework.stereotype.Service

@Service
class CouponIssuePolicyReader(
    private val couponRepository: CouponRepository,
    private val couponCacheRepository: CouponCacheRepository,
    private val cacheProperties: CacheProperties,
) {

    fun get(id:Long): CouponPolicy = couponCacheRepository.getIssuePolicyOrLoad(id){
        Thread.sleep(cacheProperties.simulatedLoadLatencyMs)    //로컬 DB라서 읽는 시간이 빨라 일부러 100ms 정도 늦춰서 테스트

         couponRepository.findById(id)
            .orElseThrow { CouponNotFoundException() }
            .let(CouponPolicy::from)

    }
}