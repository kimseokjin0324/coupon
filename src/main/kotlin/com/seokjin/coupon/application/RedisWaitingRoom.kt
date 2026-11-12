package com.seokjin.coupon.application

import com.seokjin.coupon.infrastructure.cache.WaitingRoomProperties
import com.seokjin.coupon.infrastructure.cache.WaitingRoomRedisRepository
import com.seokjin.coupon.support.WaitingRoomNotEnteredException
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class RedisWaitingRoom(
    private val waitingRoomRedisRepository: WaitingRoomRedisRepository,
    private val waitingRoomProperties: WaitingRoomProperties
) : WaitingRoom {
    override fun enter(couponId: Long, userId: Long): Admission {
        val (admitted, position) = waitingRoomRedisRepository.enter(couponId, userId)
        return if (admitted) Admission.ADMITTED
        else Admission.waiting(position, waitingRoomProperties.admitPerSecond)
    }

    override fun status(couponId: Long, userId: Long): Admission =
        when (val position = waitingRoomRedisRepository.status(couponId, userId)) {
            null -> throw WaitingRoomNotEnteredException()
            0L -> Admission.ADMITTED
            else -> Admission.waiting(position, waitingRoomProperties.admitPerSecond)
        }


    override fun isAdmitted(couponId: Long, userId: Long): Boolean =
        waitingRoomRedisRepository.isAdmitted(couponId, userId)

    @Scheduled(fixedRate = 1000)
    @SchedulerLock(name = "waiting-room-drain-lock", lockAtLeastFor = "PT0.95S")
    fun drain() {
        waitingRoomRedisRepository.activeRooms().forEach { couponId ->
            waitingRoomRedisRepository.drain(
                couponId,
                waitingRoomProperties.admitPerSecond,
                waitingRoomProperties.passTtlMs,
            )
        }
    }
}