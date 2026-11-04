package com.seokjin.coupon.api.dto

import com.seokjin.coupon.application.CacheMetricsSnapshot

class CacheMetricResponse(
    val couponDbReads :Long,
    val couponCacheHits: Long,
    val soldOutRedisExists:Long,
    val soldOutFastPathHits:Long,
) {

    companion object {
        fun from(snapShot: CacheMetricsSnapshot): CacheMetricResponse=
            CacheMetricResponse(
                couponDbReads = snapShot.couponDbReads,
                couponCacheHits=snapShot.couponCacheHits,
                soldOutRedisExists=snapShot.soldOutRedisExists,
                soldOutFastPathHits=snapShot.soldOutFastPathHits,
            )
    }
}