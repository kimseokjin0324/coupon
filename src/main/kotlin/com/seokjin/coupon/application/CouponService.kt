package com.seokjin.coupon.application

import com.seokjin.coupon.api.dto.CreateCouponRequest
import com.seokjin.coupon.domain.Coupon
import com.seokjin.coupon.domain.CouponRepository
import com.seokjin.coupon.domain.Issuance
import com.seokjin.coupon.domain.IssuanceRepository
import com.seokjin.coupon.support.AlreadyIssuedException
import com.seokjin.coupon.support.CouponNotFoundException
import com.seokjin.coupon.support.NotStartedException
import com.seokjin.coupon.support.SoldOutException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
class CouponService(
    private val couponRepository: CouponRepository,
    private val issuanceRepository: IssuanceRepository,
    private val couponIssuer: CouponIssuer
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
        val coupon = couponRepository.findById(couponId)
            .orElseThrow { CouponNotFoundException() }

        val now = LocalDateTime.now()

        if (!coupon.isBookingOpen(now)) {
            throw NotStartedException()
        }

        if (coupon.isSoldOut()) {
            throw SoldOutException()
        }

        if (issuanceRepository.existsByUserIdAndCouponId(userId, couponId)) {
            throw AlreadyIssuedException()
        }

        couponIssuer.tryIssue(couponId)
        couponRepository.incrementIssuedQuantity(couponId)


        return issuanceRepository.save(
            Issuance(
                userId = userId,
                couponId = couponId,
                issuedAt = now,
                expiredAt = now.plusDays(coupon.validityDays.toLong()),
            )
        )
    }

}