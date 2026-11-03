package com.seokjin.coupon.infrastructure.messaging

import java.time.LocalDateTime

class IssuanceRequested (
    val couponId :Long,
    val userId :Long,
    val issuedAt: LocalDateTime,
    val expiredAt: LocalDateTime,
){

}
