package com.seokjin.coupon.batch

class ReconcileReport(
    val autoFixed: Int,
    val driftAlerts: Int,
    val redisDbDrift: Long,
)