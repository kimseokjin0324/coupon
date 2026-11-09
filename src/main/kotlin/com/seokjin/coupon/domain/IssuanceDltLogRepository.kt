package com.seokjin.coupon.domain

import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

@Repository
interface IssuanceDltLogRepository : JpaRepository<IssuanceDltLog, Long> {

    fun existsByMessageKey(messageKey: String): Boolean
    fun findTop100ByOrderByReceivedAtDesc(): List<IssuanceDltLog>

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT log FROM IssuanceDltLog log WHERE log.id in :ids")
    fun findAllByIdForUpdate(@Param("ids") ids: Collection<Long>): List<IssuanceDltLog>
}