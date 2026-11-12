package com.seokjin.coupon.api.dto

import com.seokjin.coupon.application.Admission

class WaitingRoomResponse(
    val admitted: Boolean,  //통과여부
    val position: Long,     //순번
    val estimatedWaitSeconds: Long,//통과시간
) {
    companion object {
        fun from(admission: Admission): WaitingRoomResponse = WaitingRoomResponse(
            admitted = admission.admitted,
            position = admission.position,
            estimatedWaitSeconds = admission.estimatedWaitSeconds,
        )
    }
}