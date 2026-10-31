package com.seokjin.coupon.api

import com.seokjin.coupon.api.dto.IssuanceResponse
import com.seokjin.coupon.application.IssuanceService
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/issuances")
class IssuanceController(
    private val issuanceService: IssuanceService
) {
    @PostMapping("/{issuanceId}/use")
    fun use(
        @PathVariable issuanceId: Long,
        @RequestHeader("X-User-Id") userId: Long,
    ): IssuanceResponse{
        val issuance = issuanceService.use(issuanceId,userId)
        return IssuanceResponse.from(issuance)
    }
}