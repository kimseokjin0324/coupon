package com.seokjin.coupon.api

import com.seokjin.coupon.api.dto.CouponResponse
import com.seokjin.coupon.api.dto.CreateCouponRequest
import com.seokjin.coupon.api.dto.IssuanceResponse
import com.seokjin.coupon.application.CouponService
import com.seokjin.coupon.application.WaitingRoom
import com.seokjin.coupon.support.NoWaitingRoomPassException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/coupons")
class CouponController(
    private val couponService: CouponService,
    private val waitingRoom: WaitingRoom,
) {

    @PostMapping
    fun create(@RequestBody request: CreateCouponRequest): ResponseEntity<CouponResponse> {
        val coupon = couponService.createCoupon(request)
        return ResponseEntity.status(HttpStatus.CREATED).body(CouponResponse.from(coupon))
    }


    @PostMapping("/{couponId}/issue")
    fun issue(
        @PathVariable couponId: Long,
        @RequestHeader("X-User-Id") userId: Long,
    ): IssuanceResponse {
        if (!waitingRoom.isAdmitted(couponId, userId)) throw NoWaitingRoomPassException()
        val issuance = couponService.issue(couponId, userId)
        return IssuanceResponse.from(issuance)

    }

}