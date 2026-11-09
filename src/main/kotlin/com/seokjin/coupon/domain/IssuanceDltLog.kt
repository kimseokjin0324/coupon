package com.seokjin.coupon.domain

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(
    name = "issuance_dlt_log",
    indexes = [
        Index(name = "idx_issuance_dlt_message_key", columnList = "message_key")
    ]
)
class IssuanceDltLog(
    @Column(nullable = false, length = 300)
    var messageKey: String,

    @Column(nullable = false, columnDefinition = "TEXT")
    var payload: String,
    @Column(columnDefinition = "TEXT")
    var errorMessage: String? = null,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    var status: IssuanceDltStatus = IssuanceDltStatus.PENDING,
    @Column(nullable = false)
    var receivedAt: LocalDateTime,

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null
)