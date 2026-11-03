package com.seokjin.coupon.infrastructure.messaging

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.stereotype.Component


private val log = KotlinLogging.logger {}


@Component
class IssuanceWriter(
    private val transactional: IssuanceTransactionalWriter,
) {

    fun write(event: IssuanceRequested) {
        try{
        transactional.insertAndIncrement(event)
        }catch (e: DataIntegrityViolationException){    //유니크제약조건 중복 인서트시 스프링에서 제공하는 exception
            log.debug { "UNIQUE 위반은 멱등 처리: couponId=${event.couponId}, userId=${event.userId}" }
        }
    }
}