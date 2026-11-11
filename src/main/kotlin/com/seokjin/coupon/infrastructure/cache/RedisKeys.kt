package com.seokjin.coupon.infrastructure.cache

object RedisKeys {
    fun couponStock(couponId: Long) = "coupon_$couponId:stock"
    fun couponUsers(couponId: Long) = "coupon_$couponId:users"
    fun couponSoldOut(couponId: Long) = "coupon_$couponId:sold_out"
    val reconcileKey = "coupon:reconcile:recent"
}
