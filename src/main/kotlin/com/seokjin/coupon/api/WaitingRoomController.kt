package com.seokjin.coupon.api

import com.seokjin.coupon.api.dto.WaitingRoomResponse
import com.seokjin.coupon.application.WaitingRoom
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/waiting-room")
class WaitingRoomController(
    private val waitingRoom: WaitingRoom,
) {

    @PostMapping("/{couponId}")
    fun enter(
        @PathVariable(name = "couponId") couponId: Long,
        @RequestHeader("X-User-Id") userId: Long,
    ): WaitingRoomResponse = WaitingRoomResponse.from(waitingRoom.enter(couponId, userId))

    @GetMapping("/{couponId}")
    fun status(
        @PathVariable(name = "couponId") couponId: Long,
        @RequestHeader("X-User-Id") userId: Long,
    ): WaitingRoomResponse = WaitingRoomResponse.from(waitingRoom.status(couponId, userId))
}