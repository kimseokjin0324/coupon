# 5단계: 정합성 (DLT Replay, Reconcile)

## 1. 문제

4단계에서 캐시와 매진 플래그로 효율성을 개선했지만, **Redis와 DB 간 정합성**이 보장되지 않는다.

### 1-1. 발생 가능한 불일치 시나리오

| 시나리오 | Redis 상태 | DB 상태 | 증상 |
|---|---|---|---|
| **Kafka 메시지 처리 실패** | 재고 차감, 사용자 추가됨 | 발급 기록 없음 | 사용자는 발급받았다고 생각하지만 DB에 없음 |
| **Redis 장애 복구 후** | 사용자 명단 누락 | 발급 기록 있음 | 중복 발급 차단 실패 |
| **트랜잭션 롤백** | 재고 이미 차감됨 | INSERT 롤백됨 | 재고 불일치 |

### 1-2. part-5-0: 불일치 주입으로 문제 재현

테스트 스크립트 `scripts/load/part-5/run.sh`로 의도적 불일치 주입:

#### 시나리오 1: force_dlt.sh (Kafka 실패 → DLT 발생)
```bash
# Redis: 재고 -10, 사용자 10명 추가
redis_cli DECRBY "coupon_1:stock" 10
redis_cli SADD "coupon_1:users" 900001 900002 ... 900010

# DB 저장 실패 가정 → DLT 토픽에 메시지 전송
kafka-console-producer --topic issuance.requested.DLT
```

**결과**:
```
총 수량          : 5000
DB 발급 수       : 0 (ISSUED 0)
Redis 잔여 재고  : 4990
Redis 사용자 수  : 10
DB 불일치        : 10      ← Redis 재고 - DB 발급 수
```

#### 시나리오 2: force_db_only.sh (Redis 사용자 누락)
```bash
# DB: 발급 10건 직접 INSERT
INSERT INTO issuance (user_id, coupon_id, ...) VALUES ...;
UPDATE coupon SET issued_quantity = issued_quantity + 10;

# Redis: 재고만 차감, 사용자 명단에는 추가 안 함
redis_cli DECRBY "coupon_1:stock" 10
```

**결과**:
```
총 수량          : 5000
DB 발급 수       : 10 (ISSUED 10)
Redis 잔여 재고  : 4990
Redis 사용자 수  : 0
사용자 불일치    : 10      ← DB 발급 수 - Redis 사용자 수
```

---

## 2. 원인

### 2-1. Kafka 메시지 처리 실패 시 손실

기존 흐름 (part-3 ~ part-4):
```
CouponService.issue()
  ├─ Redis: 재고 차감, 사용자 추가  ✅
  └─ Kafka Producer: 메시지 전송
       ↓
  IssuanceWorker (Consumer)
    └─ DB INSERT + issued_quantity++
         ↓
       ❌ 실패 (DB 장애, 네트워크 오류)
         → 메시지 손실 (최대 3번 재시도 후 버려짐)
```

**문제점**:
- 사용자 입장: Redis에 기록됨 → **발급 성공**으로 보임
- 실제: DB에 기록 없음 → **쿠폰 사용 불가**, `GET /api/users/me/issuances`에도 안 보임

### 2-2. Redis 휘발성 특성

Redis는 **in-memory 저장소**:
- 재시작 시 데이터 손실 가능 (persistence 설정에 따라)
- 메모리 부족 시 eviction 정책에 따라 데이터 삭제
- `coupon_{id}:users` Set이 날아가면 → 중복 발급 차단 실패

---

## 3. 선택한 방법

### 3-1. 단계별 개선 전략

| 단계 | 구현 | 해결하는 문제 | 브랜치 |
|---|---|---|---|
| **part-5-0** | 불일치 주입 스크립트 | 문제 재현 | `part-5-0-drift-injection` |
| **part-5-1** | DLT Replay | Kafka 실패 메시지 복구 | `part-5-1-dlt-reply` |
| **part-5-2** | Reconcile Batch | Redis 사용자 누락 자동 보정 | `part-5-2-reconcile` |

---

## 3-2. part-5-1: DLT (Dead Letter Table) Replay

### 개념

**DLT (Dead Letter Topic/Table)**:
- Kafka Consumer가 처리 실패한 메시지를 **별도 토픽/테이블에 보관**
- 나중에 수동으로 재처리 (Replay) 가능
- 메시지 손실 방지

### 아키텍처

```
[정상 흐름]
issuance.requested 토픽
    ↓
IssuanceWorker (Consumer)
    └─ IssuanceWriter.write()
         └─ DB INSERT ✅

[실패 시 DLT 흐름]
issuance.requested 토픽
    ↓
IssuanceWorker
    └─ DB INSERT ❌ (3번 재시도 실패)
         ↓
    KafkaErrorHandlerConfig
         └─ DeadLetterPublishingRecoverer
              └─ issuance.requested.DLT 토픽으로 전송
                   ↓
              IssuanceDltLogConsumer
                   └─ issuance_dlt_log 테이블에 저장

[관리자 Replay]
POST /admin/issuance/dlt/replay {"ids": [1,2,3]}
    ↓
IssuanceDltReplayService
    ├─ issuance_dlt_log에서 payload 조회
    ├─ JSON → IssuanceRequested 객체 변환
    └─ issuance.requested 토픽에 재전송 (동기 전송)
         ↓
    IssuanceWorker
         └─ DB INSERT ✅ (정상 처리)
```

### 핵심 구현

#### 1) Kafka Error Handler 설정

**KafkaErrorHandlerConfig.kt**
```kotlin
@Bean
fun errorHandler(template: KafkaTemplate<String,Any>): DefaultErrorHandler {
    val recoverer = DeadLetterPublishingRecoverer(template) { record, _ ->
        TopicPartition("${record.topic()}.DLT", record.partition())
        // issuance.requested → issuance.requested.DLT
    }

    return DefaultErrorHandler(recoverer, FixedBackOff(1_000L, 3L))
    //                                    1초 간격으로 3번 재시도
}
```

**동작**:
- 메시지 처리 실패 시 → 1초 간격으로 3번 재시도
- 3번 모두 실패 시 → `issuance.requested.DLT` 토픽으로 전송

#### 2) DLT 메시지 수신 및 저장

**IssuanceDltLogConsumer.kt**
```kotlin
@KafkaListener(
    topics = [IssuanceTopics.REQUESTED_DLT],
    groupId = "issuance-dlt-log",
    containerFactory = "dltKafkaListenerContainerFactory"  // ByteArray 처리용
)
fun consume(record: ConsumerRecord<String, ByteArray>) {
    issuanceDltService.record(record)
}
```

**IssuanceDltService.record()**
```kotlin
@Transactional
fun record(record: ConsumerRecord<String, ByteArray>) {
    val messageKey = "${record.topic()}:${record.partition()}:${record.offset()}"
    // 예: "issuance.requested.DLT:0:5"

    if (issuanceDltLogRepository.existsByMessageKey(messageKey)) return  // 중복 방지

    issuanceDltLogRepository.save(
        IssuanceDltLog(
            messageKey = messageKey,
            payload = record.value().toString(StandardCharsets.UTF_8),  // JSON 문자열
            errorMessage = header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
            status = PENDING,
            receivedAt = LocalDateTime.now(),
        )
    )
}
```

**issuance_dlt_log 테이블**
```sql
CREATE TABLE issuance_dlt_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    message_key VARCHAR(300) NOT NULL,          -- "topic:partition:offset"
    payload TEXT NOT NULL,                       -- JSON 문자열
    error_message TEXT,                          -- 예외 메시지
    status VARCHAR(16) NOT NULL,                 -- PENDING, REPLAYED
    received_at TIMESTAMP NOT NULL,
    INDEX idx_issuance_dlt_message_key (message_key)
);
```

#### 3) DLT Replay API

**AdminIssuanceDtlController.kt**
```kotlin
@PostMapping("/replay")
fun replay(@RequestBody request: ReplayIssuanceDltRequest): IssuanceDltReplayResponse =
    IssuanceDltReplayResponse(issuanceDltReplayService.replay(request.ids))
```

**IssuanceDltReplayService.replay()**
```kotlin
@Transactional
fun replay(ids: List<Long>): Int {
    // 1. 검증
    require(selectedIds.isNotEmpty())           // 최소 1개
    require(selectedIds.size <= 100)            // 최대 100개 (폭주 방지)

    // 2. DB에서 조회 (비관적 락으로 동시성 제어)
    val logs = issuanceDltLogRepository.findAllByIdForUpdate(selectedIds)
    //         SELECT * FROM issuance_dlt_log WHERE id IN (...) FOR UPDATE

    check(logs.size == selectedIds.size)        // 존재 여부 확인

    // 3. 상태 검증 & JSON → 객체 변환
    val events = logs.map { log ->
        check(log.status == PENDING) {
            "Only Pending DLT logs can be replayed : ${log.id}"
        }
        jsonMapper.readValue(log.payload, IssuanceRequested::class.java)
    }

    // 4. Kafka 재전송 + 상태 업데이트
    logs.zip(events).forEach { (log, event) ->
        issuanceRequestProducer.publishAndWait(event)  // 동기 전송 (10초 timeout)
        log.status = REPLAYED
    }

    return logs.size
}
```

**핵심 포인트**:
1. **FOR UPDATE 락**: 동시에 여러 관리자가 같은 DLT 로그를 replay하지 못하도록 차단
2. **동기 전송 (publishAndWait)**: Kafka 브로커가 ACK 응답할 때까지 대기
   - 비동기 전송 시: 메시지 전송 전에 `status=REPLAYED` 변경 → 실패 시 손실
   - 동기 전송 시: 전송 성공 확인 후에만 `status=REPLAYED` 변경 → 안전
3. **멱등성 보장**: `IssuanceWriter`에서 UNIQUE 제약 위반 시 조용히 무시
   ```kotlin
   try {
       transactional.insertAndIncrement(event)
   } catch (e: DataIntegrityViolationException) {  // UNIQUE 제약 위반
       if (!transactional.isAlreadyApplied(event)) throw e
       log.debug { "멱등 처리: 이미 저장된 발급" }
   }
   ```

### 코드 위치

| 파일 | 역할 |
|---|---|
| `KafkaErrorHandlerConfig.kt` | DLT 토픽 전송 설정 (3번 재시도 후 전송) |
| `IssuanceDltLogConsumer.kt` | DLT 토픽 메시지 수신 |
| `IssuanceDltService.kt` | DLT 로그 저장 |
| `IssuanceDltLog.kt` | DLT 로그 엔티티 |
| `AdminIssuanceDtlController.kt` | DLT 조회/Replay API |
| `IssuanceDltReplayService.kt` | Replay 로직 (재전송) |
| `IssuanceRequestProducer.kt` | `publishAndWait()` 동기 전송 |

---

## 3-3. part-5-2: Reconcile Batch

### 개념

**대사(Reconcile)**:
- Redis(발급 판정의 진실)와 DB(영속 기록)의 수치를 **주기적으로 맞춰 보는 배치**
- 원인을 특정할 수 있는 불일치는 **자동 보정**, 특정할 수 없는 불일치는 **알람만**
- DLT Replay(5-1)가 "DB에 못 들어간 메시지"를 다룬다면, 대사는 "Redis 쪽이 틀어진 상태"를 다룬다

### 아키텍처

```
[발급 시 — 대사 대상 등록]
issue.lua (원자 실행)
    ├─ DECR stock
    ├─ SADD coupon_{id}:users
    └─ ZADD coupon:reconcile:recent <발급시각ms> <couponId>   ← 추가
                                    (member=couponId, score=최신 발급 시각)

[주기 대사 — 매 interval(기본 60초)]
Reconciler.scheduleRecent()
    ├─ cutoff = now - gracePeriod(기본 10초)
    ├─ from   = cutoff - interval × 2
    ├─ ZRANGEBYSCORE (from, cutoff] → 최근 발급된 couponId 목록
    └─ couponId마다 CouponReconciler.reconcile()

[수동 전체 대사]
POST /admin/reconcile/run
    └─ Reconciler.auditAll()  → 모든 쿠폰 대상
         → { autoFixed, driftAlerts, redisDbDrift }
```

### 핵심 구현

#### 1) 대사 대상 인덱스: 발급 Lua에 ZADD 추가

**issue.lua**
```lua
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
-- 대사 sliding window 인덱스: 쿠폰별 최신 발급 시각을 score 로 (member 는 couponId, ZADD 가 갱신).
redis.call('ZADD', KEYS[4], ARGV[3], ARGV[4])
```

- 발급과 **같은 Lua 안에서** 기록 → 발급은 됐는데 인덱스에 안 남는 경우가 없다
- member가 couponId라서 발급이 아무리 많아도 ZSET 크기 = 최근 발급이 있었던 쿠폰 수

#### 2) 대사 window 계산

**Reconciler.kt**
```kotlin
@Scheduled(fixedRateString = "\${coupon.reconcile.interval-ms}")
fun scheduleRecent() {
    val cutoffMs = System.currentTimeMillis() - reconcileProperties.gracePeriodMs
    val fromMs = cutoffMs - reconcileProperties.intervalMs * 2 //누락방지
    val couponIds = reconcileRedisRepository.couponIdsIssuedBetween(fromMs, cutoffMs)
    reconcileCoupons(couponIds)
}
```

| 설정 | 기본값 | 의미 |
|---|---|---|
| `coupon.reconcile.interval-ms` | 60000 | 배치 주기 = window 한 칸 너비 |
| `coupon.reconcile.grace-period-ms` | 10000 | 방금 발급돼 Consumer가 아직 DB에 못 쓴 쿠폰을 제외 (오탐 방지) |

- **grace**: Redis 발급 → Kafka → DB 저장 사이의 지연 동안은 원래 수치가 안 맞는다. 이 구간을 대사하면 정상 상태를 불일치로 오판한다
- **window = interval × 2**: 배치가 한 회차 밀리거나 실패해도 그 구간을 다음 회차가 다시 덮는다 (겹쳐서 보는 것은 보정이 멱등이라 안전)
- 쿠폰 하나에서 예외가 나도 `try/catch`로 알람 처리 후 다음 쿠폰 계속 → 다음 회차에 재시도

#### 3) 불일치 판정과 보정 범위

**CouponReconciler.reconcile()** 판정식:

```
dbDrift   = total_quantity - (issued_quantity + Redis stock)
userDrift = total_quantity - (Redis users 수  + Redis stock)
```

정상이라면 둘 다 0이다. (Redis에서 차감된 만큼 DB 발급 수와 Redis 명단이 늘어야 함)

| 순서 | 상황 | 처리 | 이유 |
|---|---|---|---|
| 0 | 쿠폰 없음 / Redis stock 키 없음 | 알람 | 비교 기준 자체가 없음 |
| 1 | `dbDrift ≠ 0` 또는 `stock < 0` | **알람만, 이후 보정 중단** | DB 저장 실패(→ DLT)인지 Redis 재고 오차감인지 대사만으로 구분 불가. 재고를 임의로 되돌리면 초과 발급 위험 |
| 2 | `stock > 0`인데 매진 플래그 있음 | 플래그 삭제 (자동 보정) | 재고가 남았는데 매진 응답 → 발급 기회 손실 |
| 2 | `stock = 0`인데 매진 플래그 없음 | 플래그 설정 (자동 보정) | 4단계 매진 차단이 동작하지 않음 |
| 3 | `userDrift > 0` 이고 `(DB 발급자 − Redis 명단) 수 = userDrift` | 누락자 `SADD` (자동 보정) | DB 기준으로 누락자를 **정확히 특정** 가능, `SADD`는 멱등 |
| 3 | `userDrift > 0`인데 누락자 수 ≠ drift | 알람 | 원인이 하나로 설명되지 않음 |
| 3 | `userDrift < 0` | 알람 | Redis 명단이 DB보다 많음 → 중복 발급 차단 대상이 잘못 들어가 있을 수 있음 |

```kotlin
if (userDrift > 0) {
    val redisUserIds = reconcileRedisRepository.userIds(couponId).mapNotNull { it.toLongOrNull() }
    val missingUserIds = issuanceRepository.findUserIdsByCouponId(couponId) - redisUserIds
    if (missingUserIds.size.toLong() == userDrift) {
        reconcileRedisRepository.addUsers(couponId, missingUserIds)   // SADD
        autoFixed++
    } else {
        driftAlert = true
    }
}
```

**핵심 포인트**:
1. **보정은 "DB → Redis" 한 방향, 그리고 멱등한 연산만**: `SADD`, 매진 플래그 SET/DEL. 몇 번을 실행해도 결과가 같다
2. **DB 측 불일치는 사람에게 넘긴다**: 자동으로 고칠 근거가 없는 불일치를 고치면 정합성 문제를 하나 더 만든다. 복구 경로는 DLT Replay
3. **누락자 수 = drift 수일 때만 보정**: "누락 때문에 생긴 drift"라는 게 수치로 증명될 때만 손댄다

### 코드 위치

| 파일 | 역할 |
|---|---|
| `issue.lua` | 발급 시 `coupon:reconcile:recent` ZSET에 쿠폰 등록 |
| `RedisKeys.kt` | `reconcileKey = "coupon:reconcile:recent"` |
| `Reconciler.kt` | `@Scheduled` 최근 대사, `auditAll()` 전체 대사, 결과 집계 |
| `CouponReconciler.kt` | 쿠폰 1건 판정 + 자동 보정 / 알람 |
| `CouponReconcileRedisRepository.kt` | window 조회, stock/users 조회, `SADD`, 매진 플래그 SET/DEL |
| `IssuanceRepository.kt` | `findUserIdsByCouponId()` — DB 발급자 목록 |
| `ReconcileProperties.kt` | `interval-ms`, `grace-period-ms` 설정 |
| `AdminReconcileController.kt` | `POST /admin/reconcile/run` 수동 전체 대사 |

---

## 4. 고려한 대안

### 4-1. 메시지 손실 방지 방법

| 대안 | 장점 | 선택한/하지 않은 이유 |
|---|---|---|
| **DLT + Replay** (선택) | 실패 메시지 보관 + 수동 재처리 가능 | ✅ 선택: 명시적 관리 가능, 실패 원인 분석 가능 |
| Transactional Outbox | DB 트랜잭션과 메시지 발행을 원자적으로 처리 | 복잡도 높음, 별도 폴링/CDC 필요 |
| Kafka Transaction | Producer-Consumer 간 정확히 한 번 보장 | 성능 오버헤드, 설정 복잡도 높음 |
| 무한 재시도 | 실패 시 계속 재시도 | 독성 메시지(poison message)가 Consumer 차단 |

### 4-2. DLT 저장소 선택

| 대안 | 장점 | 선택한/하지 않은 이유 |
|---|---|---|
| **DB 테이블** (선택) | SQL 조회 가능, 트랜잭션 지원, 관리 도구 풍부 | ✅ 선택: 검색/필터링/통계 분석 용이 |
| Kafka Topic만 사용 | 별도 저장소 불필요 | 오래된 메시지 조회 어려움, offset 관리 복잡 |
| S3/Object Storage | 대용량 저장 저렴 | 실시간 조회 어려움, Replay 구현 복잡 |

### 4-3. Replay 방식

| 대안 | 장점 | 선택한/하지 않은 이유 |
|---|---|---|
| **Kafka 재전송** (선택) | 기존 Consumer 재사용, 코드 중복 없음 | ✅ 선택: 일관된 처리 흐름 |
| 직접 DB INSERT | Kafka 우회, 빠름 | Redis 재고/사용자 명단 동기화 누락 위험 |
| 별도 Replay Worker | 일반 Consumer와 분리 | 코드 중복, 복잡도 증가 |

### 4-4. 대사 실행 방식

| 대안 | 장점 | 선택한/하지 않은 이유 |
|---|---|---|
| **주기 배치 `@Scheduled`** (선택) | 구현 단순, 발급 경로에 부하 없음 | ✅ 선택: 발급 hot path를 건드리지 않고 사후 검증 |
| 발급마다 실시간 검증 | 불일치 즉시 감지 | 발급 경로에 DB 조회가 추가돼 3~4단계에서 줄인 응답 시간이 다시 늘어남 |
| Spring Batch | 재시작·이력 관리 기능 | 쿠폰 단위 짧은 작업에 비해 메타 테이블·설정이 과함 |

### 4-5. 대사 대상 선정

| 대안 | 장점 | 선택한/하지 않은 이유 |
|---|---|---|
| **최근 발급 인덱스 (ZSET sliding window)** (선택) | 발급이 있었던 쿠폰만 점검, Lua에서 원자 기록 | ✅ 선택: 점검 비용이 전체 쿠폰 수가 아니라 최근 발급 쿠폰 수에 비례 |
| 매 회차 전체 쿠폰 스캔 | 누락 없음, 구현 단순 | 쿠폰이 늘수록 매분 Redis·DB 조회가 비례 증가 → 수동 `auditAll()`로만 제공 |
| DB `issued_at` 기준 조회 | Redis 추가 키 불필요 | Redis만 발급되고 DB 저장이 실패한 쿠폰(가장 잡고 싶은 케이스)이 빠짐 |

### 4-6. 불일치 보정 범위

| 대안 | 장점 | 선택한/하지 않은 이유 |
|---|---|---|
| **Redis 측만 자동 보정, DB 측은 알람** (선택) | 근거 있는 보정만 수행 | ✅ 선택: 누락자를 DB로 정확히 특정할 수 있는 경우에만 멱등 연산으로 보정 |
| DB 측도 자동 보정 (Redis 재고를 DB에 맞춤) | 사람 개입 없음 | DLT에 대기 중인 발급이 있으면 재고를 되돌린 만큼 **초과 발급** 발생 |
| 전부 알람만 | 가장 안전 | 원인이 명확한 Redis 명단 누락까지 사람이 처리 → 운영 부담, 그동안 중복 발급 차단 실패 |

---

## 5. 결과 (Before / After)

### 5-1. part-5-0: 불일치 주입 테스트

**실행**: `bash ./scripts/load/part-5/run.sh`

#### 시나리오 1: force_dlt.sh
```
===== 쿠폰 1 상태 =====
  총 수량          : 5000
  DB 발급 수       : 0 (ISSUED 0)
  Redis 잔여 재고  : 4990
  Redis 사용자 수  : 10
  매진 표시        : 0
  DB 불일치        : 10
  => DB 저장 실패: DLT 재처리 대상
```

#### 시나리오 2: force_db_only.sh
```
===== 쿠폰 1 상태 =====
  총 수량          : 5000
  DB 발급 수       : 10 (ISSUED 10)
  Redis 잔여 재고  : 4990
  Redis 사용자 수  : 0
  매진 표시        : 0
  사용자 불일치    : 10
  => Redis users 누락: 대사 자동 보정 대상
```

### 5-2. part-5-1: DLT Replay 검증

**실행**: `bash ./scripts/load/part-5/run.sh`

**테스트 시나리오**:
1. `force_dlt.sh`로 DLT 메시지 1건 주입
2. `GET /admin/issuance/dlt`로 DLT 로그 조회
3. `POST /admin/issuance/dlt/replay {"ids":[1]}`로 재처리
4. DB에 발급 기록 생성 확인
5. DLT 로그 상태가 `REPLAYED`로 변경 확인

**결과**:
```
##### DLT replay #####
  ✓ 통과  원래 발급으로 DB 저장 (1)
  ✓ 통과  DLT 로그 상태 (REPLAYED)

===== part-5-1 검증: 모두 통과 =====
```

**검증 SQL**:
```sql
-- 발급 기록 확인
SELECT COUNT(*) FROM issuance
WHERE user_id = 900001 AND coupon_id = 1;
-- 결과: 1

-- DLT 로그 상태 확인
SELECT status FROM issuance_dlt_log WHERE id = 1;
-- 결과: REPLAYED
```

### 5-3. part-5-2: Reconcile Batch

**실행**: `bash ./scripts/load/part-5/run.sh`

**결과**:
```
##### 최근 대사 대상 등록 #####
  ✓ 통과 발급 쿠폰을 최근 대사 대상으로 등록

##### Redis users 누락 자동 보정 #####
  ✓ 통과 발급자 명단 복구 (10)
  ✓ 통과 자동 보정 횟수 (1)

##### DB 측 불일치는 알람만 #####
  ✓ 통과 DB 측 불일치 감지 (10)
  ✓ 통과 Redis 재고 유지 (4990)

===== part-5-2 검증: 모두 통과 =====
```

| 지표 | Before | After | 비고 |
|---|---|---|---|
| Redis users 누락 자동 보정 | Redis 사용자 수 0 (사용자 불일치 10) | 발급자 명단 10명 복구, 자동 보정 1회 | `force_db_only` 시나리오 |
| DB 불일치 감지 | DB 불일치 10 (수동 확인만 가능) | 대사에서 10건 감지, 자동 보정 안 함 (알람만) | `force_dlt` 시나리오 |
| DB 측 불일치 시 Redis 재고 | 4990 | 4990 (그대로 유지) | 재고를 임의로 되돌리지 않음 |
| 보정 소요 시간 | - | TODO(측정 필요) | 10건 기준 |

---

## 6. 트레이드오프 / 남은 한계

### 6-1. DLT Replay의 한계

1. **수동 개입 필요**: 관리자가 직접 API 호출해야 함
   - 자동화 개선: Slack 알림 + 원클릭 Replay 버튼
   - 또는 특정 조건 시 자동 Replay (단, 위험성 검토 필요)

2. **Replay 순서 보장 어려움**:
   - 같은 사용자의 여러 쿠폰 발급이 DLT에 쌓였을 때 순서가 중요할 수 있음
   - 현재: `receivedAt` 순서로 정렬해서 replay
   - 개선: 우선순위 큐, 의존성 그래프 분석

3. **Redis 사용자 누락은 해결 못 함**:
   - `force_db_only` 시나리오 (DB엔 있는데 Redis Set에 없음)
   - → **part-5-2 Reconcile Batch**로 해결 예정

### 6-2. 다음 단계로 이어지는 이유

**part-5-2에서 해결할 문제**:
- Redis `coupon_{id}:users` Set이 날아갔을 때 자동 복구
- 주기적으로 Redis ↔ DB 불일치 검증
- 자동 보정 가능 여부 판단 (Redis 측 누락만 보정, DB 측은 알람)

---

## 7. 강의 외 추가 구현

**없음** (강의 내용 그대로 구현)

---

## 8. 트러블슈팅

### 8-1. Redis 키 형식 불일치로 재고 -10 발생

**증상**:
```bash
$ ./scripts/load/part-5/run.sh
Redis 잔여 재고: -10  ❌
```

**원인**:
- 어제 Redis 키 형식을 `coupon:` → `coupon_`로 변경
- 애플리케이션 코드 (`RedisKeys.kt`)는 `coupon_1:stock` 사용
- part-5 스크립트들은 `coupon:1:stock` 참조 (업데이트 안 됨)
- 결과: 존재하지 않는 키 `DECRBY` → **0 - 10 = -10**

**해결**:
```bash
# scripts/load/part-5/*.sh 파일에서 일괄 치환
sed -i 's/"coupon:/"coupon_/g' drift_report.sh force_db_only.sh force_dlt.sh run.sh
```

**수정 위치** (10곳):
- `drift_report.sh`: line 17-19
- `force_db_only.sh`: line 33
- `force_dlt.sh`: line 27, 32-33
- `run.sh`: line 60, 68, 71

### 8-2. 패키지 경로 불일치로 source_stage 감지 실패

**증상**:
```bash
$ ./scripts/load/part-5/run.sh
현재 소스는 part-5-0이지만 실행 중인 coupon-service는 part-5-1입니다.
```

**원인**:
- `run.sh`의 `source_stage()` 함수가 **강의 패키지명**으로 하드코딩됨
  ```bash
  if [[ -f src/main/kotlin/com/apiece/coupon/application/IssuanceDltService.kt ]]; then
  #                          ^^^^^^^ 강의 패키지
  ```
- 실제 프로젝트는 `com/seokjin/coupon` 사용
- 결과: `IssuanceDltService.kt`가 존재하는데도 **파일을 못 찾아서** part-5-0 반환

**해결**:
```bash
# run.sh line 76, 78 수정
sed -i 's|com/apiece/coupon|com/seokjin/coupon|g' ./scripts/load/part-5/run.sh
```

### 8-3. JSON 필드명 불일치 (expiresAt vs expiredAt)

**증상** (예상):
- part-5-1 Replay 시 역직렬화 실패 가능

**원인**:
- `force_dlt.sh` 초기 버전이 `"expiresAt"` 사용
- `IssuanceRequested` DTO와 `Issuance` 엔티티는 `expiredAt` 사용

**해결**:
- `force_dlt.sh` line 41 수정 (이미 수정됨)
  ```bash
  # Before
  printf '{"couponId":%s,...,"expiresAt":"%s"}\n'

  # After
  printf '{"couponId":%s,...,"expiredAt":"%s"}\n'
  ```

**확인**:
```bash
$ grep "expiresAt\|expiredAt" ./scripts/load/part-5/force_dlt.sh
printf '{"couponId":%s,"userId":%s,"issuedAt":"%s","expiredAt":"%s"}\n' \
```

---

## 참고: run.sh 스크립트 구조

### 단계 감지 로직

```bash
source_stage() {
  # 소스 코드 파일 존재 여부로 판단
  if [[ -f src/main/kotlin/com/seokjin/coupon/batch/Reconciler.kt ]]; then
    printf 'part-5-2'
  elif [[ -f src/main/kotlin/com/seokjin/coupon/application/IssuanceDltService.kt ]]; then
    printf 'part-5-1'
  else
    printf 'part-5-0'
  fi
}

runtime_stage() {
  # 실행 중인 서비스 API 응답으로 판단
  if curl -fsS "$BASE/metrics/reconcile" >/dev/null 2>&1; then
    printf 'part-5-2'
  elif curl -fsS "$BASE/admin/issuance/dlt" >/dev/null 2>&1; then
    printf 'part-5-1'
  else
    printf 'part-5-0'
  fi
}

# 검증: 소스와 런타임이 일치하지 않으면 에러
if [[ "$source" != "$runtime" ]]; then
  printf '현재 소스는 %s이지만 실행 중인 coupon-service는 %s입니다.\n' "$source" "$runtime" >&2
  printf './gradlew jibDockerBuild 후 docker compose up -d --force-recreate coupon-service를 실행하세요.\n' >&2
  exit 1
fi
```

### 테스트 함수

| 함수 | 단계 | 동작 |
|---|---|---|
| `baseline()` | part-5-0 | 불일치 주입 (force_dlt, force_db_only) |
| `dlt_replay()` | part-5-1 | DLT Replay 검증 |
| `reconcile()` | part-5-2 | Reconcile Batch 검증 |

---

## 다음 단계

**part-5-2: Reconcile Batch**
- Redis ↔ DB 불일치 자동 감지
- Redis 사용자 누락 → DB에서 복구
- DB 불일치 → 알람만 (자동 수정 안 함, 위험)
- 주기적 실행 (Cron) 또는 수동 API 호출
