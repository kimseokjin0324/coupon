# CLAUDE.md

선착순 쿠폰 발급 시스템을 단계별로 개선하면서, **"왜 다음 단계가 필요했는지"를 측정값으로 기록**하는 학습 프로젝트입니다.
최종 목표는 이 기록을 이력서·포트폴리오(README, 블로그)로 옮기는 것입니다.

---

## 프로젝트 개요

- **도메인**: 선착순 쿠폰 생성 → 발급 → 사용
- **스택**: Java / Spring Boot, MySQL, Redis, Kafka, Docker (Jib), k6
- **환경**: Windows + Git Bash (IntelliJ 터미널 셸 = Git Bash)

### 주요 API

| Method | Path | 설명 |
|---|---|---|
| POST | `/api/coupons` | 쿠폰 생성 |
| POST | `/api/coupons/{couponId}/issue` | 쿠폰 발급 (`X-User-Id` 헤더) |
| POST | `/api/issuances/{issuanceId}/use` | 쿠폰 사용 |
| GET | `/api/users/me/issuances` | 내 발급 목록 |

### 자주 쓰는 명령

```bash
./gradlew jibDockerBuild          # 앱 이미지 빌드 (Docker Desktop 실행 필요)
docker compose up -d              # 전체 기동
docker compose logs --tail 100 app
docker compose down -v            # ⚠️ DB 볼륨까지 초기화
bash ./scripts/api.sh             # API 스모크 테스트
k6 run ./k6/issue.js              # 부하 테스트
```

---

## 단계 로드맵

각 단계는 **앞 단계의 한계**에서 출발한다.

| # | 단계 | 주제 | 상태 | Git 태그 |
|---|---|---|---|---|
| 1 | 기능 요구사항 | REST API, MySQL | ✅ 완료 | `v1-basic` |
| 2 | 동시성 | 비관락, Redis 원자 연산(Lua) | ✅ 완료 | `v2-concurrency` |
| 3 | 응답성 | 큐 분리, Kafka | ⬜ | `v3-response` |
| 4 | 효율성 | 캐시, 매진 플래그 | ⬜ | `v4-efficiency` |
| 5 | 정합성 | 보상, 대사 | ⬜ | `v5-consistency` |
| 6 | 가용성 | 대기실, Rate Limit | ⬜ | `v6-availability` |

단계 기록은 `docs/stages/0N-<주제>.md`에 작성한다.

---

## 기록 규칙 (Claude가 반드시 지킬 것)

1. **숫자는 절대 지어내지 않는다.** TPS, P95, CPU, 발급 수 등은 사용자가 실제 측정해서 준 값만 쓴다. 값이 없으면 `TODO(측정 필요)`로 남긴다.
2. **문제 → 원인 → 선택 → 대안 → 결과 → 남은 한계** 순서를 지킨다. "강의에서 그렇게 해서"는 이유가 될 수 없다. 로그·지표 근거를 요구한다.
3. **선택하지 않은 대안과 그 이유**를 반드시 적는다. (면접 질문 1순위)
4. **남은 한계는 다음 단계의 "문제"와 연결**한다.
5. **강의 외 추가 구현**은 별도 섹션에 표시한다. 차별화 포인트다.
6. 단계가 끝나면 `docs/resume.md`의 이력서 문장 초안을 갱신하고, Git 태그를 달도록 안내한다.
7. 사용자가 측정 결과(k6 출력, 로그, SQL 결과)를 붙여 넣으면 해당 단계 문서의 표에 반영한다.

---

## 단계 기록 템플릿 (`docs/stages/0N-<주제>.md`)

```markdown
# N단계: <주제>

## 1. 문제
- 이전 단계에서 관찰한 현상 (숫자 포함)

## 2. 원인
- 로그/지표로 확인한 근거 (어디가 병목인지)

## 3. 선택한 방법
- 무엇을, 어떻게 (핵심 코드/설정 위치 링크)

## 4. 고려한 대안
| 대안 | 장점 | 선택하지 않은 이유 |
|---|---|---|

## 5. 결과 (Before / After)
| 지표 | Before | After |
|---|---|---|
| TPS | | |
| P95 (ms) | | |
| DB CPU (%) | | |
| Redis CPU (%) | | |
| 정합성 검증 | | |

## 6. 트레이드오프 / 남은 한계
- → 다음 단계로 이어지는 이유

## 7. 강의 외 추가 구현
- 

## 8. 트러블슈팅
- 증상 / 원인 / 해결
```

---

## 부하 테스트 표준 조건

모든 단계는 **같은 조건**으로 비교한다.

- **부하**: 5,000 req/s × 30초 = 총 150,000 요청, 요청마다 새 사용자
- **k6 executor**: `constant-arrival-rate`를 쓴다. VU 고정 방식은 쓰지 않는다.
- **테스트 전 초기화**: DB 발급 데이터, Redis 재고/발급 Set 리셋
- **환경 기록**: CPU, 메모리, 단일 머신 여부 (로컬은 절대값보다 상대 비교에 의미)
- **응답 구분**: 품절(정상 실패, 예: 409)과 서버 에러(5xx)를 구분해서 집계

### 정합성 검증 (둘 다 통과해야 성공)

```sql
SELECT c.total_quantity,
       c.issued_quantity,
       (SELECT COUNT(*) FROM issuance i WHERE i.coupon_id = c.id) AS actual
FROM coupon c WHERE c.id = 1;
```

- `issued_quantity = actual` (Lost update 없음)
- `actual <= total_quantity` (초과 발급 없음)

---

## 환경 메모 (Windows)

- `.sh`는 Git Bash로 실행한다. PowerShell 5.1은 `&&`를 지원하지 않는다.
- 한글 JSON 요청이 400으로 깨지면 `--data-binary @-` heredoc을 쓰거나 `export LANG=C.UTF-8`을 설정한다.
- `jq` 필요: `winget install jqlang.jq`
- `jibDockerBuild` 전에 Docker Desktop이 **Engine running** 상태인지 확인한다.
- 스키마가 바뀌었는데 예전 컬럼이 남아 있으면 `docker compose down -v`로 초기화한다. (`ddl-auto: update`는 컬럼을 삭제하지 않음)
