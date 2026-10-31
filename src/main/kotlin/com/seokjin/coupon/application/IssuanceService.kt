package com.seokjin.coupon.application

import com.seokjin.coupon.support.AlreadyUsedException
import com.seokjin.coupon.support.ExpiredException
import com.seokjin.coupon.support.IssuanceNotFoundException
import com.seokjin.coupon.domain.Issuance
import com.seokjin.coupon.domain.IssuanceRepository
import com.seokjin.coupon.domain.IssuanceStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
class IssuanceService(
    private val issuanceRepository: IssuanceRepository
) {

    @Transactional
    fun use(issuanceId: Long, userId: Long): Issuance {
        val issuance = issuanceRepository.findById(issuanceId).orElseThrow { IssuanceNotFoundException() }

        when (issuance.status) {
            IssuanceStatus.USED -> throw AlreadyUsedException()
            IssuanceStatus.EXPIRED -> throw ExpiredException()
            IssuanceStatus.ISSUED -> Unit
        }

        val now = LocalDateTime.now()
        if (issuance.isExpired(now)) {
            throw ExpiredException()
        }

        issuance.markUsed(now)
        return issuance
    }

    fun findByUser(userId: Long): List<Issuance> = issuanceRepository.findByUserIdOrderByIssuedAtDesc(userId)

}
