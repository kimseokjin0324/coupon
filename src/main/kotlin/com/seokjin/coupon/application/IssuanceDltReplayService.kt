package com.seokjin.coupon.application

import com.seokjin.coupon.domain.IssuanceDltLog
import com.seokjin.coupon.domain.IssuanceDltLogRepository
import com.seokjin.coupon.domain.IssuanceDltStatus
import com.seokjin.coupon.infrastructure.messaging.IssuanceRequestProducer
import com.seokjin.coupon.infrastructure.messaging.IssuanceRequested
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.json.JsonMapper

@Service
class IssuanceDltReplayService(
    private val issuanceDltLogRepository: IssuanceDltLogRepository,
    private val issuanceRequestProducer: IssuanceRequestProducer,
) {

    private val jsonMapper = JsonMapper.builder().findAndAddModules().build()

    @Transactional
    fun replay(ids: List<Long>): Int {
        val selectedIds = ids.distinct()
        require(selectedIds.isNotEmpty()) { "At least one DLT Long id is required" }
        require(selectedIds.size <= 100) { "At most $MAX_REPLAY_COUNT Dlt logs can be replayed" }

        val logs = issuanceDltLogRepository.findAllByIdForUpdate(selectedIds).sortedBy { it.receivedAt }
        check(logs.size == selectedIds.size) { "Some DLT logs were not found" }

        val events = logs.map { log ->
            check(log.status == IssuanceDltStatus.PENDING) {
                "Only Pending DLT logs can be replayed : ${log.id}"
            }
            log.toEvent()
        }

        logs.zip(events).forEach { (log, event) ->
            issuanceRequestProducer.publishAndWait(event)
            log.status = IssuanceDltStatus.REPLAYED
        }
        return logs.size
    }

    private fun IssuanceDltLog.toEvent(): IssuanceRequested =
        jsonMapper.readValue(payload, IssuanceRequested::class.java)


    private companion object {
        const val MAX_REPLAY_COUNT = 100

    }
}