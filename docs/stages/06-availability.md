# 6단계: 가용성 (대기실, Rate Limit)

## 1. 문제

5단계까지는 발급 API로 들어온 요청을 **전부 그대로 처리**한다. 순간 트래픽이 몰려도 서버 앞에서 유입량을 조절하는 장치가 없다.

### 1-1. part-6-0: 대기실 없는 발급 트래픽 급증 (베이스라인)

**실행**: `./scripts/load/part-6/run.sh` (자동 판별 → `baseline`)

**흐름**:
1. `wait_ready`: 앱 준비 확인
2. `reset.sh`: DB `coupon`/`issuance` TRUNCATE, Redis FLUSHDB
3. `create_coupon.sh`: 수량 1,000,000 쿠폰 생성 (측정 중 매진되지 않도록)
4. `k6 run scripts/load/part-6/issue_flood.js`: `constant-arrival-rate`, 요청마다 새 userId로 `POST /api/coupons/{id}/issue`

#### 측정 1: RATE=1000, DURATION=20s (스크립트 기본값)

> ⚠️ CLAUDE.md 표준 조건(5,000 req/s × 30초)이 **아니다**. 스크립트 기본값으로 실행한 결과다.

```
scenarios: issue_flood: 1000.00 iterations/s for 20s (maxVUs: 1000-4000)

THRESHOLDS
  dropped_iterations  ✓ 'count==0' count=0
  issue_failures      ✓ 'count==0' count=0

http_req_duration : avg=16.15ms min=516.9µs med=2.11ms max=855.6ms p(90)=3.67ms p(95)=29.85ms
http_req_failed   : 0.00%  0 out of 20000
http_reqs         : 20000  999.847568/s
dropped_iterations: 0
vus               : max=158
vus_max           : 1000
```

| 지표 | 값 |
|---|---|
| 부하 | 1,000 req/s × 20s |
| 총 요청 (= 발급 API 도착 수) | 20,000 |
| 실제 처리율 | 999.85 req/s |
| 2xx 외 응답 (`issue_failures`) | 0 |
| dropped_iterations | 0 |
| P50 / P90 / P95 (ms) | 2.11 / 3.67 / 29.85 |
| Max (ms) | 855.6 |
| 최대 동시 VU | 158 |
| DB CPU (%) | TODO(측정 필요) |
| Redis CPU (%) | TODO(측정 필요) |
| 정합성 SQL (`issued_quantity` / `actual`) | TODO(측정 필요) |
| 환경 (CPU / 메모리 / 단일 머신) | TODO(측정 필요) |

**관찰**:
- 1,000 req/s에서는 실패 0건, 누락 0건으로 **모두 처리됐다**. 이 부하에서는 대기실이 필요하다는 근거가 아직 나오지 않는다.
- P50 2.11ms 대비 P95 29.85ms, Max 855.6ms로 꼬리 지연이 크다. 원인은 아직 확인하지 않았다. (워밍업 구간 JIT/커넥션 풀 생성 등 가능성, 근거 없음)

#### 측정 2: 표준 조건 RATE=5000, DURATION=30s

```bash
RATE=5000 DURATION=30s ./scripts/load/part-6/run.sh baseline
```

| 지표 | 값 |
|---|---|
| 총 요청 / 실제 처리율 | TODO(측정 필요) |
| 2xx 외 응답 (품절 409 / 5xx 구분) | TODO(측정 필요) |
| dropped_iterations | TODO(측정 필요) |
| P95 / Max (ms) | TODO(측정 필요) |
| DB / Redis CPU (%) | TODO(측정 필요) |
| 정합성 SQL | TODO(측정 필요) |

---

## 2. 원인

- TODO: 표준 조건 측정에서 지연·실패가 어디서 생기는지 (앱 스레드, Redis, Kafka Producer, DB 커넥션 등) 로그/지표 근거

## 3. 선택한 방법

| 단계 | 구현 | 해결하는 문제 | 브랜치 |
|---|---|---|---|
| **part-6-0** | 대기실 없는 발급 부하 측정 | 문제 재현 (베이스라인) | `part-6-0-load-test` |
| **part-6-1** | Redis 대기실 (전역 통과 속도) | 발급 API 도착량을 서버 수와 무관하게 초당 N명으로 제한 | `part-6-1-waiting-room` |
| **part-6-2** | Gateway Rate Limit | 한 사용자의 반복 요청(어뷰징)을 앱 앞단에서 429로 차단 | `part-6-2-edge-rate-limit` |

### 3-1. part-6-1: Redis 대기실

- 진입 `POST /api/waiting-room/{couponId}` → `waiting-room-enter.lua`: Redis `TIME`을 score로 `ZADD NX` (서버가 여러 대여도 단일 시계 FIFO)
- 상태 `GET /api/waiting-room/{couponId}` → `waiting-room-status.lua`: 입장권 있으면 통과, 없으면 순번
- 드레인 `RedisWaitingRoom.drain()`: `@Scheduled(fixedRate = 1000)` + ShedLock(`waiting-room-drain-lock`)으로 **한 서버만** 매초 실행 → `waiting-room-drain.lua`가 줄 앞에서 `admit-per-second`명을 꺼내 입장권(`SET PX passTtlMs`) 발급
- 발급 API 게이트: `CouponController.issue()`에서 입장권 없으면 `403 NO_WAITING_ROOM_PASS`
- 설정: `coupon.waiting-room.admit-per-second=100`, `pass-ttl-ms=30000`

| 파일 | 역할 |
|---|---|
| `WaitingRoomController.kt` | 진입 / 상태 API |
| `RedisWaitingRoom.kt` | 진입·상태·통과 여부, 매초 드레인 |
| `WaitingRoomRedisRepository.kt` | 키 설계 (`waiting:{couponId}:queue`, `waiting:{couponId}:pass:<userId>`, `waiting:room`) |
| `ShedLockConfig.kt` | Redis 기반 분산 락 (드레인 중복 실행 방지) |
| `lua/waiting-room-*.lua` | 진입 / 상태 / 드레인 원자 연산 |

### 3-2. part-6-2: Gateway Rate Limit

- 별도 모듈 `gateway/` (Spring Cloud Gateway, WebFlux), 포트 `8090 → 8080`
- 공개 사용자 API만 라우팅: `/api/waiting-room/**`, `/api/coupons/*/issue`, `/api/issuances/*/use`, `/api/users/me/issuances`. 관리자·메트릭 API는 라우팅하지 않는다.
- `RequestRateLimiter` + `RedisRateLimiter` (Token Bucket), 키는 `userIdKeyResolver`(`user:<X-User-Id>`) → **사용자마다 별도 버킷**
  - 헤더가 없거나 양의 정수가 아니면 `Mono.empty()` → Gateway 기본값(`deny-empty-key=true`)에 따라 거부 (응답 코드는 TODO(확인 필요))
- 설정: `replenishRate=5`/s, `burstCapacity=10`, `requestedTokens=1` (`RATE_LIMIT_REPLENISH`, `RATE_LIMIT_BURST`로 오버라이드)
- 한도 초과 시 Gateway가 **429**로 응답해 coupon-service까지 요청이 가지 않는다.

| 파일 | 역할 |
|---|---|
| `gateway/src/main/kotlin/com/seokjin/gateway/GatewayApplication.kt` | Gateway 앱, `userIdKeyResolver` |
| `gateway/src/main/resources/application.yml` | 라우트, Rate Limit 설정 |
| `docker-compose.yml` (`gateway` 서비스) | Redis·coupon-service 연결, 한도 환경변수 |

## 4. 고려한 대안

| 대안 | 장점 | 선택하지 않은 이유 |
|---|---|---|
| TODO | | |

## 5. 결과 (Before / After)

| 지표 | Before (part-6-0) | After |
|---|---|---|
| TPS | TODO(표준 조건 측정 필요) — 참고: 1,000 req/s 조건 999.85 | |
| P95 (ms) | TODO(표준 조건 측정 필요) — 참고: 1,000 req/s 조건 29.85 | |
| DB CPU (%) | TODO(측정 필요) | |
| Redis CPU (%) | TODO(측정 필요) | |
| 정합성 검증 | TODO(측정 필요) | |

### 5-1. part-6-1: 서버 수와 무관한 전역 통과 속도

**실행**: `./scripts/load/part-6/run.sh single`, `./scripts/load/part-6/run.sh scale`
**조건**: 대기실 진입 1,000 req/s × 20s (스크립트 기본값, 요청마다 새 userId), `admit-per-second=100`, 허용 오차 ±30% (70~130건/s)
**측정 방법**: k6 종료 후 Redis `waiting:{couponId}:pass:*` 키 수 ÷ 20초. 입장권 TTL(30s) > 측정 시간(20s)이라 측정 중 발급된 입장권은 만료되지 않는다.

| 지표 | 서버 1대 (`single`) | 서버 2대 (`scale`) |
|---|---|---|
| 진입 요청 수 / 처리율 | 20,001 / 999.97 req/s | 20,001 / 999.90 req/s |
| 진입 실패 (`waiting_room_enter_failures`) | 0 | 0 |
| 진입 P50 / P90 / P95 (ms) | 1.05 / 1.61 / 3.14 | 1.05 / 1.60 / 2.10 |
| 진입 Max (ms) | 111.3 | 75.01 |
| Redis 입장권 발급 | 2,000건 | 2,000건 |
| 통과 속도 | **약 100건/s** ✓ | **약 100건/s** ✓ |

**관찰**:
- 서버를 2대로 늘려도 통과 속도가 100건/s로 같다. 드레인이 ShedLock으로 한 서버에서만 돌기 때문에, 서버 수만큼 통과 속도가 배로 늘지 않는다. (드레인이 서버마다 돌았다면 2대에서 약 200건/s가 나왔어야 한다)
- 진입 요청 1,000 req/s 중 초당 100명만 발급 API로 넘어간다. 즉 발급 API 도착량이 대기실 설정값으로 제한된다.
- 진입 API는 Redis Lua 한 번이라 P95 2~3ms 수준이다.

### 5-2. part-6-1: 실제 사용자 여정 (`journey`)

**실행**: `./scripts/load/part-6/run.sh journey`
**조건**: 사용자 200명이 동시에 1회씩 `POST 진입 → GET 상태 폴링(1s + 지터 0~0.25s) → 통과 후 POST 발급`. `admit-per-second=100`, 최대 대기 30s

| 지표 | 값 |
|---|---|
| 발급 성공 (`journey_issued_users`) | **200 / 200** |
| 실패 (`journey_failures`) | 0 |
| 전체 HTTP 요청 | 675 (= 진입 200 + 폴링 275 + 발급 200) |
| 폴링 요청 (`journey_status_polls`) | 275 |
| 사용자당 폴링 횟수 avg / max | 1.375 / 2 |
| 대기 시간 avg / med / P95 / max | 1.67s / 1.34s / 2.46s / 2.54s |
| 폴링 응답 P95 / max (ms) | 11.25 / 25.08 |
| 전체 HTTP P95 / max (ms) | 153.3 / 227.27 |
| 전체 소요 | 2.5s |

**관찰**:
- 200명이 초당 100명씩 통과하므로 마지막 사용자는 약 2초 후 통과할 것으로 예상된다. 실측 max 대기 2.54s로 이와 맞다.
- 폴링 때문에 늘어난 요청은 사용자 1명당 평균 1.375건이다. 발급 1건을 위해 클라이언트가 보내는 요청은 평균 3.375건(진입 1 + 폴링 1.375 + 발급 1)이다.
- 폴링 P95는 11.25ms인데 전체 HTTP P95는 153.3ms다. 진입 또는 발급 쪽이 느린 것으로 보이지만, 태그별(`waiting_room_enter` / `coupon_issue`) 지연은 이번 출력에 없어서 확인하지 않았다. → TODO(측정 필요): `k6 run --summary-trend-stats` 또는 태그별 threshold로 분리

### 5-3. part-6-2: Gateway Rate Limit (어뷰저 컷 + 정상 통과)

**실행**: `./scripts/load/part-6/run.sh` (자동 판별 → `gateway`)
**조건**: Gateway(`:8090`)로 10초간 동시 전송, `replenishRate=5`/s, `burstCapacity=10`
- 어뷰저: 같은 userId 1개로 200 req/s
- 정상: 매 요청 다른 userId로 20 req/s

**어뷰저 통과 기대값**: `burst + replenish × 초 = 10 + 5 × 10 = 60`건 (허용 ±20%: 48~72)

| 지표 | 어뷰저 (같은 id) | 정상 (매번 다른 id) |
|---|---|---|
| 전송 | 2,001 (200 req/s) | 201 (20 req/s) |
| 통과 (`*_passed`) | **60** (기대값 60) | **201** |
| 차단 429 (`*_blocked`) | **1,941** (97.0%) | **0** |
| 그 외 실패 (`*_failed`) | 0 | 0 |
| 실제 통과율 | 6.00/s | 20.09/s |

| 지표 | 값 |
|---|---|
| 전체 요청 / dropped_iterations | 2,202 / 0 |
| 전체 HTTP P95 / max (ms) | 6.69 / 39.02 |
| 통과 응답(2xx) P50 / P95 / max (ms) | 5.89 / 15.14 / 39.02 |

> `http_req_failed` 88.14%는 429 차단(1,941건)이 집계된 값이다. 서버 에러가 아니다.

**관찰**:
- 어뷰저 통과 60건이 Token Bucket 계산값(10 + 5 × 10)과 **정확히 일치**했다. 나머지 1,941건은 Gateway에서 429로 끊겨 coupon-service에 도착하지 않았다.
- 정상 사용자는 각자 버킷이라 0건 차단. 한 사용자의 폭주가 다른 사용자의 통과에 영향을 주지 않았다.
- 전체 P95가 6.69ms로, 통과 응답 P95 15.14ms보다 낮다. 429는 Gateway에서 바로 응답해서 빠르다. 통과 요청은 Gateway를 거쳐 coupon-service까지 다녀오기 때문에 이 차이가 생긴다.
- Gateway를 거칠 때 추가되는 지연은 직접 호출과 비교하지 않아서 아직 모른다. → TODO(측정 필요)

## 6. 트레이드오프 / 남은 한계

- TODO

## 7. 강의 외 추가 구현

- 없음 (현재까지)

## 8. 트러블슈팅

### 8-1. `wait_ready` "준비 실패": 내 발급 목록 API 경로 단수/복수 불일치

**증상**:
```
$ ./scripts/load/part-6/run.sh
===== part-6-0: 대기실 없는 발급 트래픽 급증 =====
http://localhost:8080 준비 실패
```
컨테이너 4개(coupon-service, kafka, mysql, redis)는 모두 Up/healthy 상태였다.

**원인**:
- 컨트롤러: `@RequestMapping("/api/users/me/issuance")` (단수). `8142da2`에서 처음 만들 때부터 단수였다.
- part-6 스크립트의 준비 확인: `GET /api/users/me/issuances` (복수) → **404**
- `wait_ready`는 `curl -fsS ... && return`이라 404를 실패로 보고 60회 재시도 후 종료
- 이전 단계에서 드러나지 않은 이유:
  - part-2 ~ part-5 부하 스크립트는 이 엔드포인트를 호출하지 않았다.
  - `scripts/api.sh`는 복수형으로 호출했지만 `curl -sS`(`-f` 없음)라 404여도 종료 코드 0이었고, 마지막 줄이라 눈에 띄지 않았다.

**해결**:
- 컨트롤러 경로를 `/api/users/me/issuances`로 변경 (CLAUDE.md API 명세, 모든 스크립트와 통일)
- `./gradlew jibDockerBuild && docker compose up -d`로 재기동

**교훈**: 스모크 테스트(`api.sh`)에 `curl -f`가 없으면 404도 성공으로 지나간다.

### 8-2. part-6 스크립트가 강의 패키지 경로(`com/apiece`)로 단계 판별

**증상**: `RedisWaitingRoom.kt`를 만들어도 `run.sh`가 `baseline`으로 판별. `journey`는 "waiting_room_journey는 part-6-1-waiting-room 이후에 실행할 수 있다"로 종료.

**원인**: 스크립트가 `src/main/kotlin/com/apiece/coupon/...` 파일 존재로 단계를 판별한다. 이 프로젝트는 `com/seokjin/coupon`이다. 5단계 8-2와 같은 원인.

**해결**:
- `run.sh`: 수정 완료 (`f174772`)
- `waiting_room_journey.sh:14`, `verify.sh:10`, `gateway_rate_limit.sh:17`: 같은 경로가 남아 있어 함께 수정
  ```bash
  sed -i 's|com/apiece/|com/seokjin/|g' scripts/load/part-6/*.sh
  ```

### 8-3. 대기실 진입 100% 실패: 컨트롤러 경로 오타

**증상**: `waiting_room_enter_failures` count=20001 (100%), P95 1.57ms
**원인**: `WaitingRoomController`가 `@RequestMapping("/api/wating-room")` (`i` 누락). 스크립트는 `/api/waiting-room` → 404 `No static resource`. 404는 Spring이 즉시 반환해 응답이 매우 빨랐다. k6 threshold 실패로 `set -e`가 걸려 입장권 검증 단계까지 가지 못했다.
**해결**: `/api/waiting-room`으로 수정 후 재빌드

### 8-4. Redis 입장권 0건: 키 이름 콜론 누락

**증상**: 진입 실패 0건인데 `Redis 입장권: 0건, 약 0건/s` → ✗ 실패
**원인**:
- `WaitingRoomRedisRepository.passKeyPreFix()`가 `"waiting:{$couponId}:pass"` (끝 `:` 없음) → 실제 키 `waiting:{1}:pass12345`
- `run.sh`의 `pass_count`는 `waiting:{1}:pass:*` 패턴으로 SCAN → 0건
- Redis 확인: `waiting:{*}:pass:*` 0개, `waiting:{*}:pass*` 3,000개. 대기실 자체는 정상 동작했고 **측정만 어긋났다**.

**해결**: 접두사를 `"waiting:{$couponId}:pass:"`로 변경. Kotlin `passKey()`와 `waiting-room-drain.lua`(`ARGV[3] .. userId`)가 같은 접두사를 쓰므로 한 곳만 고치면 양쪽이 맞춰진다.

**교훈(8-4)**: 앱 내부에서만 쓰는 키라도 외부 도구(측정 스크립트, 운영 SCAN)가 패턴으로 찾는다. 구분자 규칙(`:`)을 지켜야 `pass1`/`pass12` 같은 접두 매칭 혼동도 없다.

### 8-5. 브랜치 전환 후 경로 수정이 사라짐

**증상**: single/scale은 통과했는데, 이후 `journey`에서 "애플리케이션 준비 실패" 발생. `/api/users/me/issuances` → 404
**원인**:
- `part-6-1-waiting-room` 브랜치는 `8eda309`에서 갈라졌다. 경로 수정 커밋 `3a8b7d2`는 `part-6-0-load-test`에만 있었다.
- 브랜치를 바꾼 뒤 재빌드하면서 컨트롤러가 다시 단수형(`/issuance`)으로 배포됐다.
**해결**: `git cherry-pick 3a8b7d2` 후 재빌드
**교훈**: 단계별 브랜치로 진행할 때는 다음 단계 브랜치를 만들기 전에 앞 브랜치의 수정을 merge해 둔다.
