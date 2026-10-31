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

TODO(로그/지표로 확인 필요)

- MySQL의 `issued_quantity += 1` UPDATE가 어떤 격리 수준에서 실행되는지?
- `issuance` INSERT와 `coupon` UPDATE 사이에 race window가 있는지?
- DB 커넥션 풀 설정? (HikariCP)

## 3. 선택한 방법

TODO(구현 후 작성)

## 4. 고려한 대안

| 대안 | 장점 | 선택하지 않은 이유 |
|---|---|---|
| TODO | | |

## 5. 결과 (Before / After)

| 지표 | Before | After |
|---|---|---|
| **목표 요청** | 150,000 (5,000 req/s × 30s) | TODO(측정 필요) |
| **실제 처리** | 8,821 (147 req/s) | TODO(측정 필요) |
| **드롭율** | 139,442 / 148,263 (94.0%) | TODO(측정 필요) |
| **응답 실패율** | 26.98% (2,380 / 8,821) | TODO(측정 필요) |
| **P95 응답시간** | 45.7s | TODO(측정 필요) |
| **실제 발급 수** | 6,551 | TODO(측정 필요) |
| **초과발급** | FAIL (6,551 > 5,000, +1,551장) | TODO(측정 필요) |
| **카운터 일치** | FAIL (656 ≠ 6,551, -5,895) | TODO(측정 필요) |

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

## 6. 트레이드오프 / 남은 한계

TODO(구현 후 작성)

## 7. 강의 외 추가 구현

TODO

## 8. 트러블슈팅

TODO
