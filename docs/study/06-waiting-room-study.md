# 6단계 학습 가이드: Redis ZSET · Lua 원자성 · 대기실 · Rate Limit

> 목표: 따라 친 코드를 **내 말로 설명하고, 바꿨을 때 결과를 예측**할 수 있게 만든다.
> 방법: 직접 쳐 보기 → 일부러 깨뜨리기 → 코드 따라 읽기 → 설정 바꿔 측정 → 보지 않고 다시 쓰기
> 예상 소요: 약 3~4시간 (STEP 별로 끊어서 해도 된다)

## 체크리스트

- [ ] STEP 0. 준비 (5분)
- [ ] STEP 1. ZSET · 입장권 명령 직접 쳐 보기 (30분)
- [ ] STEP 2. Lua 없이 경쟁 상황 재현 → Lua로 해결 (30분)
- [ ] STEP 3. API로 대기실 한 바퀴 돌기 (20분)
- [ ] STEP 4. 요청 하나 따라 코드 읽기 (1시간)
- [ ] STEP 5. 설정 바꾸고 결과 예측 → 측정 (1시간)
- [ ] STEP 6. (심화) 코드 깨뜨리기: ShedLock 제거 (30분)
- [ ] STEP 7. 보지 않고 Lua 다시 쓰기 (30분)
- [ ] STEP 8. grill me Q5~Q8 이어서 하기

**규칙**: 실행하기 **전에** "예상" 칸을 먼저 채운다. 예상이 틀린 곳이 아직 모르는 곳이다.

---

## STEP 0. 준비

```bash
cd ~/Downloads/coupon/coupon
docker compose ps                       # mysql, redis, kafka, coupon-service 가 Up 인지
curl -s localhost:8080/api/users/me/issuances -H 'X-User-Id: 1'   # 200 이면 OK
```

Redis 접속 (Git Bash 에서 TTY 에러가 나면 앞에 `winpty` 를 붙인다):
```bash
docker compose exec redis redis-cli
# 또는
winpty docker compose exec redis redis-cli
```

> STEP 1~2 는 쿠폰 id **99** 를 쓴다. 앱의 드레인은 `waiting:room` Set 에 등록된 쿠폰만 처리하므로, 손으로 만든 99번 줄은 앱이 건드리지 않는다.

---

## STEP 1. ZSET · 입장권 명령 직접 쳐 보기

각 명령을 치고 **옆의 Lua 파일 몇째 줄인지** 찾아본다.
- `src/main/resources/lua/waiting-room-enter.lua`
- `src/main/resources/lua/waiting-room-status.lua`
- `src/main/resources/lua/waiting-room-drain.lua`

### 1-1. 진입 (enter.lua)

```
ZADD waiting:{99}:queue NX 100 alice
ZADD waiting:{99}:queue NX 200 bob
ZADD waiting:{99}:queue NX 300 chris
ZRANGE waiting:{99}:queue 0 -1 WITHSCORES
```

| 명령 | 예상 | 결과 |
|---|---|---|
| `ZRANK waiting:{99}:queue bob` | | |
| `ZADD waiting:{99}:queue NX 999 alice` (새로고침 흉내) | | |
| `ZRANGE waiting:{99}:queue 0 -1 WITHSCORES` → alice score | | |
| `ZADD waiting:{99}:queue 999 alice` (**NX 없이**) | | |
| `ZRANK waiting:{99}:queue alice` | | |

<details><summary>정답 확인</summary>

- `ZRANK bob` → `1` (0부터 센다. 그래서 enter.lua 에서 `rank + 1`)
- `NX` 있을 때 → `0` (이미 있어서 무시), alice score 는 `100` 그대로
- `NX` 없이 → `0` (새 member 가 아니라 0) 이지만 score 가 `999` 로 **갱신**
- 그 뒤 `ZRANK alice` → `2` (맨 뒤로 밀림) → **NX 가 왜 필요한지**
</details>

### 1-2. 드레인 + 입장권 (drain.lua)

```
ZRANGE waiting:{99}:queue 0 1
SET waiting:{99}:pass:bob 1 PX 30000
ZREM waiting:{99}:queue bob
```

| 명령 | 예상 | 결과 |
|---|---|---|
| `ZRANK waiting:{99}:queue bob` | | |
| `EXISTS waiting:{99}:pass:bob` | | |
| `PTTL waiting:{99}:pass:bob` (몇 초 뒤 다시) | | |
| `EXISTS waiting:{99}:pass:chris` | | |

<details><summary>정답 확인</summary>

- `ZRANK bob` → `(nil)`: 줄에서 지워짐 (grill me Q3 "지워진 철수")
- `EXISTS pass:bob` → `1`: 입장권으로 통과 여부를 안다
- `PTTL` → 30000 이하로 계속 줄어든다. 0 이 되면 키가 사라진다
- `EXISTS pass:chris` → `0`: 줄에 있지만 아직 통과 못 함
</details>

### 1-3. 상태 조회 (status.lua) 를 머리로 실행

`status.lua` 를 열고 아래 세 사람에 대해 반환값을 적어 본다.

| 사용자 | 예상 반환값 | 이유 |
|---|---|---|
| bob (입장권 있음) | | |
| chris (줄에 있음) | | |
| zed (아무 데도 없음) | | |

<details><summary>정답 확인</summary>

- bob → `0` (통과) / chris → 순번 (`ZRANK + 1`) / zed → `-1` (진입 안 함 → API 는 404 `WAITING_ROOM_NOT_ENTERED`)
</details>

### 정리
```
DEL waiting:{99}:queue waiting:{99}:pass:bob
```

---

## STEP 2. Lua 없이 경쟁 상황 재현 → Lua 로 해결

### 2-1. 터미널 2개로 "서버 2대가 동시에 드레인" 흉내

터미널 A, B 둘 다 redis-cli 접속. 먼저 줄을 채운다:
```
ZADD waiting:{99}:queue 1 u1 2 u2 3 u3 4 u4
```

**아래 순서를 정확히** 지킨다:

| 순서 | 터미널 A (서버 1) | 터미널 B (서버 2) |
|---|---|---|
| ① | `ZRANGE waiting:{99}:queue 0 1` | |
| ② | | `ZRANGE waiting:{99}:queue 0 1` |
| ③ | `ZREM waiting:{99}:queue u1 u2` | |
| ④ | | `ZREM waiting:{99}:queue u1 u2` |

- **예상**: A 가 꺼낸 사람 = ______ / B 가 꺼낸 사람 = ______
- **결과**: ______
- 무엇이 문제인가: __________________________

<details><summary>정답 확인</summary>

A, B 둘 다 `u1, u2` 를 꺼낸다 → **같은 사람이 두 번 통과** (입장권 두 번 발급).
"꺼내기(ZRANGE)"와 "지우기(ZREM)" 사이에 다른 서버의 명령이 **끼어들었기** 때문.
</details>

### 2-2. 실제 drain.lua 로 같은 일 하기

일반 터미널 (Git Bash) 에서:
```bash
docker compose exec -T redis redis-cli DEL 'waiting:{99}:queue'
docker compose exec -T redis redis-cli ZADD 'waiting:{99}:queue' 1 u1 2 u2 3 u3 4 u4

# 인자: [키 개수=1] [KEYS[1]=줄] [ARGV[1]=꺼낼 인원] [ARGV[2]=TTL ms] [ARGV[3]=입장권 접두사]
docker compose exec -T redis redis-cli EVAL "$(cat src/main/resources/lua/waiting-room-drain.lua)" \
  1 'waiting:{99}:queue' 2 30000 'waiting:{99}:pass:'
```

| 확인 | 예상 | 결과 |
|---|---|---|
| EVAL 반환값 | | |
| 같은 EVAL 한 번 더 | | |
| 세 번째 EVAL | | |
| `KEYS 'waiting:{99}:pass:*'` | | |

<details><summary>정답 확인</summary>

`2` → `2` → `0` (줄이 비었음). 입장권은 u1~u4 각 1개씩, 중복 없음.
Lua 스크립트는 **통째로 한 명령처럼** 실행돼서 다른 클라이언트가 중간에 끼어들 수 없다.
</details>

### 2-3. 면접 함정: Lua 는 롤백되나?

**아니다.** Lua 의 원자성 = "실행 중 다른 명령이 끼어들지 않는다". 중간에 에러가 나면 **이미 실행된 명령은 그대로 남는다.** (DB 트랜잭션의 롤백과 다름)
→ 그래서 Lua 는 **검사 먼저, 쓰기 나중** 순서로 짠다. `enter.lua` 가 `EXISTS` 검사를 맨 위에 두는 이유를 확인해 본다.

### 정리
```bash
docker compose exec -T redis redis-cli DEL 'waiting:{99}:queue'
docker compose exec -T redis redis-cli --scan --pattern 'waiting:{99}:pass:*' | xargs -r docker compose exec -T redis redis-cli DEL
```

---

## STEP 3. API 로 대기실 한 바퀴 돌기

```bash
./scripts/load/reset.sh >/dev/null
CID=$(./scripts/load/create_coupon.sh); echo "coupon=$CID"
U=7
```

| # | 명령 | 예상 (HTTP 코드 / 응답) | 결과 |
|---|---|---|---|
| 1 | `curl -s -w ' %{http_code}\n' -X POST localhost:8080/api/coupons/$CID/issue -H "X-User-Id: $U"` (줄 안 서고 발급) | | |
| 2 | `curl -s -w ' %{http_code}\n' localhost:8080/api/waiting-room/$CID -H "X-User-Id: $U"` (진입 전 상태 조회) | | |
| 3 | `curl -s -w ' %{http_code}\n' -X POST localhost:8080/api/waiting-room/$CID -H "X-User-Id: $U"` (진입) | | |
| 4 | `sleep 2` 후 2번 다시 (상태 조회) | | |
| 5 | 1번 다시 (발급) | | |
| 6 | `docker compose exec -T redis redis-cli PTTL "waiting:{$CID}:pass:$U"` | | |
| 7 | 1번 한 번 더 (같은 사람 재발급) | | |

<details><summary>정답 확인</summary>

1. `403` `NO_WAITING_ROOM_PASS` — 입장권 없음
2. `404` `WAITING_ROOM_NOT_ENTERED` — status.lua 가 `-1`
3. `200` `admitted:false, position:1`
4. `200` `admitted:true` — 드레인(1초마다)이 꺼내고 입장권 발급
5. `200` 발급 성공
6. 30000 이하의 남은 ms
7. 입장권은 아직 있지만 **중복 발급 방지**(2단계 Redis Set)에 걸린다 → 응답 코드를 직접 확인해서 적기
</details>

### 3-1. (덤) Gateway 로 헤더 없이 보내면?

```bash
docker compose up -d gateway
curl -s -i -X POST localhost:8090/api/waiting-room/$CID | head -1
```
- 결과 코드: ______ → `docs/stages/06-availability.md` 3-2 의 `TODO(확인 필요)` 에 채울 값

---

## STEP 4. 요청 하나 따라 코드 읽기

**파일 단위가 아니라 요청 경로 단위로** 읽는다. 각 줄 옆에 **내 말로** 주석을 달아 본다 (커밋하지 않아도 됨).

| 경로 | 따라갈 순서 |
|---|---|
| ① 진입 | `WaitingRoomController.enter` → `RedisWaitingRoom.enter` → `WaitingRoomRedisRepository.enter` (`SADD waiting:room` + 키 생성) → `waiting-room-enter.lua` |
| ② 상태 | `WaitingRoomController.status` → `RedisWaitingRoom.status` (`null`/`0`/순번 분기) → `waiting-room-status.lua` |
| ③ 드레인 | `RedisWaitingRoom.drain` (`@Scheduled` + `@SchedulerLock`) → `activeRooms()` → `waiting-room-drain.lua` |
| ④ 발급 게이트 | `CouponController.issue` → `isAdmitted` (`hasKey`) → 없으면 `NoWaitingRoomPassException` (403) |
| ⑤ 분산 락 | `ShedLockConfig` (`RedisLockProvider`, `defaultLockAtMostFor = PT10S`) ↔ `@SchedulerLock(lockAtLeastFor = "PT0.95S")` |

**스스로 답해 볼 질문** (답을 이 파일 아래에 적어 둔다):
1. `enter()` 에서 `SADD waiting:room` 은 왜 Lua 밖에서 할까? 안에 넣으면 무엇이 달라질까?
2. 키에 `{couponId}` 처럼 **중괄호**를 쓰는 이유는? (힌트: Redis Cluster 해시 태그, Repository 주석)
3. `estimatedWaitSeconds` 는 어떤 식으로 계산되나? 200번째면 몇 초?
4. `lockAtLeastFor = 0.95초` 가 없으면 무슨 일이 생길 수 있을까? (서버 시계가 조금씩 다를 때)
5. 락을 잡은 서버가 죽으면 드레인은 최대 몇 초 멈출까?

---

## STEP 5. 설정 바꾸고 결과 예측 → 측정

설정은 **환경변수로만** 바꾼다 (재빌드 불필요). `run.sh` 도 같은 변수를 읽어 기대값을 계산한다.

### 5-1. 통과 속도를 50 으로

```bash
export COUPON_WAITING_ROOM_ADMIT_PER_SECOND=50
docker compose up -d coupon-service
./scripts/load/part-6/run.sh single
```
- 예상 입장권 수 (20초): ______ / 예상 통과 속도: ______
- 결과: ______

### 5-2. 같은 설정으로 journey

```bash
./scripts/load/part-6/run.sh journey
```
- 예상 최대 대기시간 (200명, 50명/s): ______ / 예상 사용자당 폴링 수: ______
- 결과 (`journey_waiting_time` max, `journey_polls_per_user` avg): ______
- 기준값 (100명/s): max 2.54s, 폴링 avg 1.375 (06 문서 5-2)

### 5-3. Rate Limit 한도를 10/s 로

```bash
export RATE_LIMIT_REPLENISH=10
docker compose up -d gateway
./scripts/load/part-6/run.sh gateway
```
- 예상 어뷰저 통과 수 (10초, burst 10): ______
- 결과 `abuser_passed`: ______
- 기준값 (5/s): 60 (06 문서 5-3)

### 원래대로 되돌리기 (꼭)
```bash
unset COUPON_WAITING_ROOM_ADMIT_PER_SECOND RATE_LIMIT_REPLENISH
docker compose up -d coupon-service gateway
```

---

## STEP 6. (심화) 코드 깨뜨리기: ShedLock 제거

> ⚠️ 코드 수정 + 재빌드가 필요하다. **시작 전에 지금 작업을 커밋**해 두어야 깔끔하게 되돌릴 수 있다.

1. `RedisWaitingRoom.kt` 의 `@SchedulerLock(...)` 한 줄을 주석 처리
2. 재빌드 후 실행
   ```bash
   ./gradlew :jibDockerBuild && docker compose up -d coupon-service
   ./scripts/load/part-6/run.sh scale
   ```
3. 예상 통과 속도 (서버 2대): ______ / `run.sh` 판정 (통과/실패): ______
4. 결과: ______
5. Redis 에서 중복 통과가 있었는지 생각해 보기: 입장권 키 수와 진입한 사용자 수를 비교하면? (grill me Q2 정리 참고)
6. 되돌리기
   ```bash
   git restore src/main/kotlin/com/seokjin/coupon/application/RedisWaitingRoom.kt
   ./gradlew :jibDockerBuild && docker compose up -d coupon-service
   ```

---

## STEP 7. 보지 않고 Lua 다시 쓰기

1. 새 파일 `scratch-enter.lua` 를 만들고 **주석만** 옮겨 적는다 (KEYS/ARGV/반환 형식)
2. 원본을 닫고 본문을 직접 쓴다
3. STEP 2-2 처럼 `EVAL "$(cat scratch-enter.lua)" 2 'waiting:{99}:queue' 'waiting:{99}:pass:dave' dave` 로 실행해서 원본과 결과 비교
4. `drain.lua` 도 같은 방식으로
5. 막혔던 줄: ______ ← 여기가 복습 포인트
6. 끝나면 `scratch-*.lua` 삭제

---

## STEP 8. grill me 이어서

`docs/interview/06-availability-qna.md` 의 Q5~Q8. Claude 에게 **"Q5부터 grill me 이어서 해줘"** 라고 하면 된다.

---

## 내 답 / 메모

(STEP 4 질문 답, 예상이 틀렸던 것, 새로 알게 된 것)

-
