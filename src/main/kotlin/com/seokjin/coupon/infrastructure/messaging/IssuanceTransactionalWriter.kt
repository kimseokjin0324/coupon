package com.seokjin.coupon.infrastructure.messaging

import com.seokjin.coupon.domain.CouponRepository
import com.seokjin.coupon.domain.Issuance
import com.seokjin.coupon.domain.IssuanceRepository
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class IssuanceTransactionalWriter(
    private val issuanceRepository: IssuanceRepository,
    private val couponRepository: CouponRepository
) {

    @Transactional
    fun insertAndIncrement(event: IssuanceRequested) {
        if (issuanceRepository.existsByUserIdAndCouponId(event.userId, event.couponId)) return
        issuanceRepository.save(
            Issuance(
                userId = event.userId,
                couponId = event.couponId,
                issuedAt = event.issuedAt,
                expiredAt = event.expiredAt,
            )
        )
        couponRepository.incrementIssuedQuantity(event.couponId)

    }

    fun isAlreadyApplied(event: IssuanceRequested): Boolean =
        issuanceRepository.existsByUserIdAndCouponId(event.userId, event.couponId)

}