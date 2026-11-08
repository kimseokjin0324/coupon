package com.seokjin.coupon.application

import com.seokjin.coupon.domain.Coupon
import java.time.LocalDateTime

class CouponPolicy(
    val startsAt: LocalDateTime?,
    val validityDays: Int,
) {

    fun isBookingOpen(now: LocalDateTime): Boolean =
        startsAt?.let { !now.isBefore(it) } ?: true

    companion object {
        fun from(coupon: Coupon): CouponPolicy = CouponPolicy(
            startsAt = coupon.startsAt,
            validityDays = coupon.validityDays,
        )
    }
}