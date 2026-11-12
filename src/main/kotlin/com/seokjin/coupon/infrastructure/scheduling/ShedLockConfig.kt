package com.seokjin.coupon.infrastructure.scheduling

import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.provider.redis.spring.RedisLockProvider
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory

@Configuration
@EnableSchedulerLock(defaultLockAtMostFor = "PT10S")    //ISO표기법 10초
class ShedLockConfig {

    @Bean
    fun lockProvider(connectionFactory: RedisConnectionFactory): LockProvider =
        RedisLockProvider(connectionFactory, "coupon")

}