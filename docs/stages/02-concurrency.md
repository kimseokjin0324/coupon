# 2단계: 동시성

## 1. 문제

초당 5,000건 요청 조건에서 **초과발급**과 **Lost Update** 발생:

- **초과발급**: 5,000장 재고에 6,551장 발급 (1,551장 초과, +31%)
- **카운터 불일치**: `issued_quantity=656` vs 실제 `issuance_rows=6,551` (90% lost update)
- **처리량 부족**: 목표 5,000 req/s → 실제 147 req/s (97% dropped)
- **응답 지연**: P95 응답시간 45.7초

### 테스트 조건

```bash
./scripts/load/part-2/run.sh
```

- 재고: 5,000장
- 부하: constant-arrival-rate, 5,000 req/s × 30초 = 150,000 요청
- VU: preAllocatedVUs=2000, maxVUs=5000 (실제 5000까지 도달했으나 부족)

## 2. 원인

**Race Condition**: 여러 트랜잭션이 동시에 같은 `coupon` 행을 읽고 수정

1. **Read-Modify-Write 경합**
   - `CouponRepository.findById()` (일반 SELECT): 락 없이 읽기
   - `coupon.issuedQuantity++`: JPA dirty checking으로 UPDATE
   - 10개 스레드가 동시에 `issuedQuantity=100` 읽음 → 모두 101로 UPDATE → **9번 Lost Update**

2. **재고 검증 Race Window**
   ```kotlin
   // CouponService.kt:44-45
   if (coupon.isSoldOut()) throw SoldOutException()  // ← 5000건이 여기 통과
   coupon.issuedQuantity++  // ← 실제로는 5001, 5002, ... 6551까지 증가
   ```

3. **증상**
   - 초과발급: 6,551 > 5,000 (+31%)
   - Lost Update: 656 ≠ 6,551 (90%의 UPDATE가 덮어써짐)

## 3. 선택한 방법

**비관적 락 (Pessimistic Lock)**: `SELECT ... FOR UPDATE`로 행 단위 배타 락

### 구현 위치

**CouponRepository.kt**:11-13
```kotlin
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT c from Coupon c where c.id = :id")
fun findByIdForUpdate(@Param("id") id: Long): Coupon?
```

**CouponService.kt**:35
```kotlin
val coupon = couponRepository.findByIdForUpdate(couponId)
    ?: throw CouponNotFoundException()
```

### 동작 방식

1. 첫 번째 트랜잭션이 `SELECT ... FOR UPDATE` 실행 → `coupon.id=1` 행에 **X-lock** 획득
2. 나머지 4,999개 트랜잭션은 락 대기 큐에서 **순차 대기**
3. 트랜잭션 커밋 시 락 해제 → 다음 트랜잭션이 락 획득
4. `issuedQuantity++` UPDATE는 **락 보호 구간**에서 실행 → Lost Update 없음

## 4. 고려한 대안

| 대안 | 장점 | 선택하지 않은 이유 |
|---|---|---|
| **낙관적 락** (`@Version`) | 락 대기 없어 처리량 높음 | 충돌 시 재시도 로직 필요, 높은 동시성 환경에서 재시도 폭증 가능 |
| **Redis 원자 연산** (INCR, Lua) | DB 부하 분산, 초당 10만+ TPS 가능 | 추가 인프라(Redis), DB와 정합성 보장 로직 필요 |
| **Application 락** (synchronized) | 구현 간단 | 단일 인스턴스만 가능, 스케일아웃 불가 |
| **Unique Index** (user_id, coupon_id) | DB 레벨 중복 방지 | 초과발급은 막지 못함, 재고 검증은 별도 필요 |

### 2단계에서 비관적 락을 선택한 이유

- **1단계에서 겪은 문제가 정합성**이므로, 처리량보다 **정확성 우선**
- 추가 인프라 없이 **MySQL만으로 해결** 가능
- 다음 단계(3단계: 응답성)에서 처리량 개선 예정

## 5. 결과 (Before / After)

| 지표 | Before | After |
|---|---|---|
| **목표 요청** | 150,000 (5,000 req/s × 30s) | 150,000 (5,000 req/s × 30s) |
| **실제 처리** | 8,821 (147 req/s) | 8,775 (162 req/s, +10%) |
| **드롭율** | 139,442 / 148,263 (94.0%) | 141,226 / 150,001 (94.1%) |
| **응답 실패율** | 26.98% (2,380 / 8,821) | 43.01% (3,775 / 8,775) |
| **P95 응답시간** | 45.7s | 48.93s (+7%) |
| **실제 발급 수** | 6,551 | 5,000 |
| **초과발급** | ❌ FAIL (6,551 > 5,000, +1,551장) | ✅ OK (5,000 = 5,000) |
| **카운터 일치** | ❌ FAIL (656 ≠ 6,551, -5,895) | ✅ OK (5,000 = 5,000) |

### k6 상세 출력 (Before)

```
WARN[0003] Insufficient VUs, reached 5000 active VUs and cannot initialize more

HTTP
http_req_duration..............: avg=20.24s min=0s      med=18.46s max=45.76s p(90)=45.64s p(95)=45.7s
  { expected_response:true }...: avg=27.72s min=1.02s   med=29.61s max=45.76s p(90)=45.69s p(95)=45.7s
http_req_failed................: 26.98% 2380 out of 8821
http_reqs......................: 8821   146.905641/s

EXECUTION
dropped_iterations.............: 139442 2322.278248/s
iteration_duration.............: avg=20.28s min=522.4µs med=18.46s max=45.76s p(90)=45.64s p(95)=45.7s
iterations.....................: 8821   146.905641/s

NETWORK
data_received..................: 1.7 MB 29 kB/s
data_sent......................: 1.1 MB 18 kB/s
```

### 정합성 검증 (Before)

```sql
+-----------------+----------------+---------------+---------------+-------------+
| issued_quantity | total_quantity | issuance_rows | over_issuance | count_match |
+-----------------+----------------+---------------+---------------+-------------+
|             656 |           5000 |          6551 | FAIL          | FAIL        |
+-----------------+----------------+---------------+---------------+-------------+
```

### k6 상세 출력 (After)

```
WARN[0003] Insufficient VUs, reached 5000 active VUs and cannot initialize more

HTTP
http_req_duration..............: avg=26.83s min=0s       med=29.93s max=50.65s p(90)=46.8s  p(95)=48.93s
  { expected_response:true }...: avg=26.05s min=558.78ms med=26.03s max=50.37s p(90)=45.65s p(95)=47.97s
http_req_failed................: 43.01% 3775 out of 8775
http_reqs......................: 8775   161.951731/s

EXECUTION
dropped_iterations.............: 141226 2606.472383/s
iteration_duration.............: avg=26.84s min=506.4µs  med=29.93s max=50.65s p(90)=46.8s  p(95)=48.93s
iterations.....................: 8775   161.951731/s

NETWORK
data_received..................: 2.1 MB 38 kB/s
data_sent......................: 1.0 MB 19 kB/s
```

### 정합성 검증 (After)

```sql
+-----------------+----------------+---------------+---------------+-------------+
| issued_quantity | total_quantity | issuance_rows | over_issuance | count_match |
+-----------------+----------------+---------------+---------------+-------------+
|            5000 |           5000 |          5000 | OK            | OK          |
+-----------------+----------------+---------------+---------------+-------------+
PASS
```

## 6. 트레이드오프 / 남은 한계

### ✅ 얻은 것

- **정합성 보장**: 초과발급 0건, Lost Update 0건
- **구현 단순성**: JPA 어노테이션 3줄로 해결, 추가 인프라 불필요

### ❌ 지불한 비용

- **처리량 여전히 낮음**: 162 req/s (목표 5,000 req/s의 3.2%)
- **응답시간 증가**: P95 45.7s → 48.93s (+7%)
- **실패율 증가**: 26.98% → 43.01% (+59%)
- **병목 지속**: 드롭율 94% 유지

### 🔍 원인 분석

비관적 락으로 **순차 처리 강제**:
- 모든 요청이 `coupon.id=1` 행 락을 기다림
- 5,000장 발급 완료까지 약 30초 소요 (5000 ÷ 162 req/s ≈ 31초)
- 나머지 144,000+ 요청은 타임아웃/드롭

### → 다음 단계로 이어지는 이유

**3단계: 응답성** - 큐 분리로 "발급 요청"과 "실제 발급 처리"를 분리
- 요청은 빠르게 응답 (큐 적재 확인만)
- 실제 발급은 백그라운드 워커가 처리 (Kafka Consumer 등)
- 목표: 사용자 응답시간 < 1초 유지하면서 처리량 향상

## 7. 강의 외 추가 구현

### Redis Lua 스크립트 방식 실험

비관적 락의 처리량 한계를 개선하기 위해 **Redis 원자 연산**으로 재구현하고 비교 측정

#### 구현

**CouponIssuer.kt**:14-29
```kotlin
private val script: RedisScript<Long> = RedisScript.of(
    ClassPathResource("lua/issue.lua"), Long::class.java
)

fun tryIssue(couponId: Long) {
    val result = redisTemplate.execute(script, listOf(stockKey(couponId)))
    when (result) {
        1L -> Unit                      // 발급 성공
        0L -> throw SoldOutException()  // 품절
    }
}
```

**issue.lua**:1-7
```lua
local remaining = tonumber(redis.call('GET', KEYS[1]) or '0')
if remaining <= 0 then
    return 0  -- 품절
end
redis.call('DECR', KEYS[1])
return 1  -- 성공
```

**CouponService.kt**:56-57
```kotlin
couponIssuer.tryIssue(couponId)  // Redis로 재고 검증 + 감소 (원자적)
couponRepository.incrementIssuedQuantity(couponId)  // DB 카운터 업데이트
```

#### 결과 비교

| 지표 | 비관적 락 | Redis Lua | 개선율 |
|---|---|---|---|
| **처리량** | 162 req/s | 195 req/s | +20% |
| **P95 응답시간** | 48.93s | 45.12s | -8% |
| **드롭율** | 94.1% | 93.5% | -0.6%p |
| **초과발급** | ✅ OK | ✅ OK | - |
| **카운터 일치** | ✅ OK | ✅ OK | - |

#### k6 상세 출력 (Redis Lua)

```
HTTP
http_req_duration..............: avg=22.8s  min=0s       med=24.95s max=46.65s p(90)=42.85s p(95)=45.12s
  { expected_response:true }...: avg=24.18s min=748.67ms med=24.46s max=46.57s p(90)=42.17s p(95)=44.41s
http_req_failed................: 48.45% 4701 out of 9701
http_reqs......................: 9701   194.891393/s

EXECUTION
dropped_iterations.............: 141298 2818.562271/s
iterations.....................: 9701   194.891393/s
```

```sql
+-----------------+----------------+---------------+---------------+-------------+
| issued_quantity | total_quantity | issuance_rows | over_issuance | count_match |
+-----------------+----------------+---------------+---------------+-------------+
|            5000 |           5000 |          5000 | OK            | OK          |
+-----------------+----------------+---------------+---------------+-------------+
PASS
```

#### 분석

**✅ 개선 효과**
- SELECT ... FOR UPDATE 락 대기 제거 → 처리량 +20%
- Redis 인메모리 연산으로 재고 검증 고속화

**⚠️ 여전히 낮은 이유**
- **DB INSERT 병목**: `issuanceRepository.save()` 여전히 동기 처리
- **중복 체크 DB 조회**: `existsByUserIdAndCouponId()` 매번 SELECT
- **트랜잭션 오버헤드**: 5,000건 모두 개별 트랜잭션 커밋

**결론**: Redis로 재고 관리를 옮겨도 **DB 쓰기 병목**이 남아 있음
→ 3단계(비동기 큐)에서 DB 쓰기를 분리해야 근본 해결 가능

## 8. 트러블슈팅

### HikariCP 커넥션 풀 부족 문제

#### 증상

최초 부하 테스트에서 강의 결과(P99 3.96s) 대비 **10배 이상 느린 응답시간**(P99 43.03s) 발생

```
HTTP
http_req_duration..............: avg=22.06s p(95)=42.32s p(99)=43.03s
http_reqs......................: 195.52 req/s
```

#### 원인 분석

**CouponService.kt:38-68** 동기 처리 경로에서 HikariCP 커넥션 풀 대기

```kotlin
@Transactional
fun issue(couponId: Long, userId: Long): Issuance {
    // 1. Redis 재고 차감 (5ms)
    couponIssuer.tryIssue(couponId)

    // 2. DB UPDATE (50ms) ← 커넥션 필요
    couponRepository.incrementIssuedQuantity(couponId)

    // 3. DB INSERT (100ms) ← 커넥션 필요
    return issuanceRepository.save(...)
}
```

Docker 로그 확인 결과:

```
Maximum pool size: undefined/unknown
```

**HikariCP 기본값 10개** vs **Tomcat 스레드 150개**
- 5,000 req/s → 2,834개 요청이 실제 처리됨
- 이 중 **140개 이상이 커넥션 대기** → 평균 22초 대기

#### 해결 시도

**application.yaml:8-11**

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 50      # 10 → 50
      minimum-idle: 10
      connection-timeout: 3000
  threads:
    virtual:
      enabled: true              # Virtual Thread 활성화
```

#### 결과 비교 (pool=10 vs pool=50)

| 지표 | pool=10 | pool=50 | 개선율 |
|---|---|---|---|
| **처리량** | 195 req/s | 2,834 req/s | **+1353%** |
| **P99 응답시간** | 43.03s | 4.3s | **-90%** |
| **P95 응답시간** | 42.32s | 3.6s | **-91%** |
| **드롭율** | 93.4% (140,055/150,003) | 36.4% (54,668/150,003) | **-57%p** |
| **성공률** | 98.2% (4,997 성공) | **2.6%** (2,483 성공) | **-95%p** ❌ |
| **에러율** | 1.8% | **97.2%** (92,691건 5xx) | **+95%p** ❌ |
| **정합성** | ✅ OK (4,997 = 4,997) | ✅ OK (2,483 = 2,483) | - |

#### k6 상세 출력 (pool=50)

```
HTTP
http_req_duration..............: avg=1.08s min=0s      med=0s     max=7s    p(90)=3.32s p(95)=3.6s
  { expected_response:true }...: avg=3.6s  min=29.77ms med=3.62s  max=7s    p(90)=4.88s p(95)=6.11s
http_req_failed................: 97.39% 92852 out of 95335
http_reqs......................: 95335  2834.06 req/s

CUSTOM
status_2xx.....................: 2483   73.81/s
status_409_sold_out_or_dup.....: 161    4.79/s
status_other (5xx/에러)........: 92691  2755.46/s  ← 97.2%가 에러!
```

#### 에러 로그

```
java.sql.SQLTransientConnectionException:
HikariPool-1 - Connection is not available, request timed out after 3000ms
(total=50, active=50, idle=0, waiting=1)
```

**분석**: 2,834 req/s 부하에서 50개 커넥션으로도 부족 → 대기 시간 3초 초과 → 타임아웃 에러

#### 트레이드오프 정리

| 설정 | 장점 | 단점 |
|---|---|---|
| **pool=10** | 안정적 (에러율 1.8%), 정합성 보장 | 처리량 매우 낮음 (195 req/s) |
| **pool=50** | 처리량 14배 증가 (2,834 req/s) | 에러율 97%, 재고 절반만 처리 |
| pool=100+ | 에러율 감소 가능 | MySQL 부하 급증, 단일 DB 한계 |

#### 근본 원인

**동기 처리 아키텍처의 한계**:
- 사용자가 DB INSERT 완료까지 대기 (평균 150ms+)
- 5,000 req/s × 150ms = **최소 750개 동시 커넥션 필요**
- 커넥션 풀을 늘려도 MySQL 단일 인스턴스가 병목

#### 결론

커넥션 풀 튜닝만으로는 **처리량과 안정성을 동시에** 달성 불가

**→ 3단계: 비동기 큐(Kafka)로 근본 해결 필요**
- API는 큐 적재만 하고 즉시 응답 (< 100ms)
- 백그라운드 Worker가 DB 처리
- 커넥션 풀 크기와 무관하게 처리량 확보
