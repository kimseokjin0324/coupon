package com.seokjin.coupon.application

import com.seokjin.coupon.api.dto.CreateCouponRequest
import com.seokjin.coupon.domain.Coupon
import com.seokjin.coupon.domain.CouponRepository
import com.seokjin.coupon.domain.Issuance
import com.seokjin.coupon.infrastructure.cache.CacheProperties
import com.seokjin.coupon.infrastructure.messaging.IssuanceRequestProducer
import com.seokjin.coupon.infrastructure.messaging.IssuanceRequested
import com.seokjin.coupon.support.CouponNotFoundException
import com.seokjin.coupon.support.NotStartedException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
class CouponService(
    private val couponRepository: CouponRepository,
    private val couponIssuePolicyReader: CouponIssuePolicyReader,
    private val couponIssuer: CouponIssuer,
    private val issuanceRequestProducer: IssuanceRequestProducer,
) {
    @Transactional
    fun createCoupon(request: CreateCouponRequest): Coupon {
        val coupon = couponRepository.save(
            Coupon(
                name = request.name,
                totalQuantity = request.totalQuantity,
                validityDays = request.validityDays,
                startsAt = request.startsAt,
            )
        )
        couponIssuer.initStock(coupon.id!!, coupon.totalQuantity)

        return coupon
    }

    @Transactional
    fun issue(couponId: Long, userId: Long): Issuance {

        val policy = couponIssuePolicyReader.get(couponId)

        val now = LocalDateTime.now()

        if (!policy.isBookingOpen(now)) {
            throw NotStartedException()
        }

        couponIssuer.tryIssue(couponId, userId)


        val expiredAt = now.plusDays(policy.validityDays.toLong())
        issuanceRequestProducer.publish(
            IssuanceRequested(
                couponId = couponId,
                userId = userId,
                issuedAt = now,
                expiredAt = expiredAt
            )
        )

        return Issuance(
            userId = userId,
            couponId = couponId,
            issuedAt = now,
            expiredAt = expiredAt,
        )
    }

}