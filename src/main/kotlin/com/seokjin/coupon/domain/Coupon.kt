package com.seokjin.coupon.domain

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(name = "coupon")
class Coupon(

    @Column(nullable = false, length = 80)
    var name: String,

    @Column(nullable = false)
    var totalQuantity: Int,

    @Column(nullable = false)
    var issuedQuantity: Int = 0,

    @Column(nullable = false)
    var validityDays: Int = 7,

    var startsAt: LocalDateTime? = null,

    @Column(nullable = false, updatable = false)
    var createdAt: LocalDateTime = LocalDateTime.now(),

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
) {

    fun isBookingOpen(now: LocalDateTime): Boolean =
        startsAt?.let { !now.isBefore(it) } ?: true

    fun isSoldOut(): Boolean = issuedQuantity >= totalQuantity

}