# 지라 백로그 — 커피숍 주문 시스템

**필수 요구사항**과 **도전 요구사항**을 분리한다.

- **필수 에픽(E0~E5)**: 기능이 동작하는 기본 구현까지. (Kafka 실시간 전송은 필수 #3에 포함)
- **도전 에픽(E6)**: 다수 인스턴스 안전성 · 동시성 · 데이터 일관성 · 테스트. 필수 위에 얹는 레이어.

> 비관적 락 / Redis 분산락 / AFTER_COMMIT·Outbox / 동시성·일관성 테스트는 **모두 도전(E6)** 으로 이동.
> 필수 에픽은 "단일 트랜잭션 기반 기본 동작"까지만 책임진다.

가져오기용 파일: [`jira-import.csv`](jira-import.csv) (Issue ID + Parent 계층형).

---

## A. 계층 목록 + 수용 기준(AC)

### [필수] E0 — 설계 및 문제해결 전략 수립  *(완료)*
- **Story** README 설계 문서 작성 — ERD/API/전략/기술선택 이유 자기완결.
  - AC: 과제 0번 체크리스트 4항목 포함.

### [필수] E1 — 프로젝트 셋업 및 공통 기반
- **Story** Gradle 의존성 및 프로파일 구성
  - Task: `web`, `data-jpa`, `validation`, `spring-kafka`, `mysql-connector-j`, (test)`h2`
  - Task: `dev`(MySQL) · `test`(H2) 프로파일 분리
  - AC: `./gradlew build` 성공. *(Redis 의존성은 도전 E6에서 추가)*
- **Story** 공통 에러 응답 및 전역 예외 처리
  - Task: `ErrorResponse{code,message}` + `@RestControllerAdvice` + 에러코드 상수
  - AC: 모든 API 동일 에러 포맷.
- **Story** 시드 데이터 및 초기 스키마
  - Task: 메뉴·사용자 시드, 스키마 생성 전략(flyway/ddl-auto)
  - AC: 기동 후 메뉴·사용자 조회 가능.
- **Story** 로컬 인프라 docker-compose (MySQL·Kafka)
  - AC: `docker compose up`으로 기동, 앱 연결. *(Redis는 도전에서 추가)*

### [필수] E2 — 메뉴 조회
- **Story** 메뉴 목록 조회 API
  - Task: entity/repository/service/controller + 응답 DTO, `@WebMvcTest`
  - AC: `GET /api/menus` 200, `[{id,name,price}]`.

### [필수] E3 — 포인트 충전 (기본)
- **Story** 포인트 충전 API
  - Task: `POST /api/points/charge` + 검증(userId/amount)
  - Task: 잔액 증가 + `point_histories(CHARGE)` 기록 (단일 트랜잭션)
  - AC: 정상 충전 반영·이력, 음수/누락 → 400. *(동시성 제어는 도전 E6)*

### [필수] E4 — 주문 및 결제 (기본 + Kafka 전송)
- **Story** 주문/결제 API
  - Task: `POST /api/orders` 트랜잭션(존재확인→총액→차감→주문/항목 저장)
  - Task: 메뉴명/단가 스냅샷, 응답 DTO(잔액·상태)
  - AC: 정상 201, 잔액 부족 → 409 `INSUFFICIENT_POINT`, 없는 메뉴/사용자 → 404.
    *(동시 결제 정합성 강화는 도전 E6)*
- **Story** 주문 이벤트 Kafka 전송 (필수 #3)
  - Task: 주문 결제 후 `order-events` 토픽으로 전송(userId/menuId/amount)
  - AC: 결제 완료 시 메시지 발행(전송 자체). *(발행-트랜잭션 정합성 강화는 도전 E6)*

### [필수] E5 — 인기 메뉴 (기본)
- **Story** 인기 메뉴 조회 API
  - Task: 최근 7일 `PAID` 주문 집계(수량합, 동점 menu_id asc, top3)
  - AC: 주문 횟수 정확, 7일 경계·동점 처리.

---

### [도전] E6 — 도전 요구사항 (필수 위에 얹는 레이어)

- **Story** ① 다수 인스턴스 무상태 설계
  - Task: 세션/로컬 캐시 제거, 상태 외부화(MySQL/Redis) 확인
  - Task: 다중 인스턴스 기동 시나리오 문서화(docker-compose scale)
  - AC: 인스턴스 증설이 기능에 영향 없음을 근거로 설명·시연 가능.
- **Story** ② 동시성 제어
  - Task: Redis 의존성 추가 + 분산락 유틸(`point:{userId}`)
  - Task: 포인트 충전/차감·주문에 **비관적 락**(`SELECT FOR UPDATE`) 적용
  - Task: 분산락으로 인스턴스 간 동일 사용자 요청 직렬화
  - AC: 동일 사용자 동시 충전/결제 N건 → 분실 갱신 없음(최종 잔액 = 기대값).
- **Story** ③ 데이터 일관성
  - Task: 차감+주문 저장을 단일 트랜잭션으로 보장, 실패 시 전체 롤백
  - Task: Kafka 발행을 `AFTER_COMMIT`으로 이동(결제 성공 건만 전송)
  - Task: (확장) Transactional Outbox 패턴 설계/구현
  - AC: 결제 롤백 시 잔액 원복 + 이벤트 미발행.
- **Story** ④ 기능 및 제약 테스트
  - Task: 단위(서비스)·슬라이스(컨트롤러)·통합(Testcontainers) 테스트
  - Task: 동시성 테스트(`ExecutorService`), 일관성(롤백↔미발행) 테스트
  - AC: 각 기능·제약에 대응하는 테스트 존재, `./gradlew test` 통과.

---

## B. Jira CSV Import 안내

- 파일: `jira-import.csv` (열: `Issue ID`, `Issue Type`, `Summary`, `Parent`, `Description`).
- 매핑: `Issue ID`→**업무 항목 ID**, `Parent`→**상위 항목**, `Issue Type`→**업무 유형**,
  `Summary`→**요약**, `Description`→**설명**.
- **주의**: `Issue Type` 값(`Epic`/`Story`)은 프로젝트의 업무 유형 이름과 일치해야 한다.
  한글 프로젝트면 `에픽`/`스토리`로 바꿔야 할 수 있다(파일에서 일괄 치환).

## C. 라벨/우선순위 가이드

- 라벨: `backend`, `challenge`(E6), `concurrency`, `kafka`.
- 순서: E0 → E1 → (E2·E3) → E4 → E5 → **E6(도전)**. 평가 핵심은 E6.
