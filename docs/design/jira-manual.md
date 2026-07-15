# 지라 수동 등록용 — 커피숍 주문 시스템 (에픽 · 스토리 · 태스크)

CSV 없이 손으로 등록하기 위한 문서. 계층: **에픽 → 스토리 → 태스크**.

- 각 항목의 **제목(코드블록)** 을 지라 생성 폼에 그대로 복사.
- 등록 순서: 에픽 먼저 → 그 아래 스토리 → 스토리 아래 태스크(하위 작업/작업).
- 태스크는 `` `제목` `` 형태로 표기했다. 필요하면 하위 작업(Sub-task)으로, 아니면 별도 작업(Task)으로.
- 필수(E1~E5)=기본 동작 / 도전(E6)=다수 인스턴스·동시성·일관성·테스트.

---

## [필수] EPIC E1 — 프로젝트 셋업 및 공통 기반

**제목**
```
[필수] 프로젝트 셋업 및 공통 기반
```
**설명**
```
의존성·프로파일·공통 응답/예외·시드 데이터·로컬 인프라 구성. 이후 모든 기능의 토대.
```

### └ Story E1-1 · Gradle 의존성 및 프로파일 구성
**제목**
```
Gradle 의존성 및 프로파일 구성
```
**설명** — 수용 기준: `./gradlew build` 성공, 프로파일별 DataSource 동작. (Redis는 E6에서 추가)

**태스크**
1. `build.gradle에 web·data-jpa·validation 의존성 추가`
2. `build.gradle에 spring-kafka·mysql-connector-j·(test)h2 의존성 추가`
3. `application-dev.yml 작성 (MySQL 연결)`
4. `application-test.yml 작성 (H2, MySQL 호환 모드)`
5. `./gradlew build 검증 및 실행 확인`

### └ Story E1-2 · 공통 에러 응답 및 전역 예외 처리
**제목**
```
공통 에러 응답 및 전역 예외 처리
```
**설명** — 수용 기준: 모든 API가 `{code, message}` 동일 포맷 반환.

**태스크**
1. `ErrorResponse{code,message} DTO 작성`
2. `ErrorCode 상수/enum 정의 (VALIDATION_ERROR 등)`
3. `BusinessException 베이스 예외 설계`
4. `GlobalExceptionHandler(@RestControllerAdvice) 작성 - 검증·도메인 예외 매핑`

### └ Story E1-3 · 시드 데이터 및 초기 스키마
**제목**
```
시드 데이터 및 초기 스키마
```
**설명** — 수용 기준: 기동 후 메뉴·사용자 조회 가능.

**태스크**
1. `스키마 생성 전략 결정 (flyway vs ddl-auto)`
2. `schema 정의/마이그레이션 작성 (7개 테이블)`
3. `data 시드 작성 (메뉴·사용자)`
4. `JpaAuditingConfig 작성 (createdAt/updatedAt 자동)`

### └ Story E1-4 · 로컬 인프라 docker-compose (MySQL·Kafka)
**제목**
```
로컬 인프라 docker-compose (MySQL·Kafka)
```
**설명** — 수용 기준: `docker compose up` 기동, 앱 연결 확인. (Redis는 E6에서 추가)

**태스크**
1. `docker-compose.yml에 MySQL 서비스 정의`
2. `docker-compose.yml에 Kafka(KRaft/Zookeeper) 서비스 정의`
3. `앱 연결 설정 및 기동 문서화 (README 실행 방법)`

---

## [필수] EPIC E2 — 메뉴 조회

**제목**
```
[필수] 메뉴 조회
```
**설명**
```
커피 메뉴 목록 조회 API.
```

### └ Story E2-1 · 메뉴 목록 조회 API
**제목**
```
메뉴 목록 조회 API
```
**설명** — 수용 기준: `GET /api/menus` 200, `[{id,name,price}]` 반환.

**태스크**
1. `Menu 엔티티 + MenuRepository 작성`
2. `MenuResponse DTO 작성 (엔티티 미노출)`
3. `MenuService 작성 (@Transactional(readOnly=true))`
4. `MenuController GET /api/menus 작성`
5. `@WebMvcTest 컨트롤러 슬라이스 테스트`

---

## [필수] EPIC E3 — 포인트 충전

**제목**
```
[필수] 포인트 충전
```
**설명**
```
포인트 충전 API 기본 구현. 단일 트랜잭션. (동시성 제어는 도전 E6)
```

### └ Story E3-1 · 포인트 충전 API
**제목**
```
포인트 충전 API
```
**설명** — 수용 기준: 정상 충전 시 잔액 반영·이력 기록, 금액 0 이하/누락 → 400.

**태스크**
1. `User 엔티티 + UserRepository 작성`
2. `PointBalance 엔티티 + PointBalanceRepository 작성`
3. `PointHistory 엔티티 + PointHistoryRepository 작성`
4. `PointChargeRequest DTO + 검증(@NotNull/@Positive)`
5. `PointService.charge 구현 (잔액 증가 + CHARGE 이력, @Transactional)`
6. `PointController POST /api/points/charge 작성`
7. `서비스 단위 테스트 + 컨트롤러 슬라이스 테스트`

---

## [필수] EPIC E4 — 주문 및 결제

**제목**
```
[필수] 주문 및 결제
```
**설명**
```
주문/결제 + 주문내역 Kafka 실시간 전송(필수 3번). 동시 결제 정합성 강화는 도전 E6.
```

### └ Story E4-1 · 주문/결제 API
**제목**
```
주문/결제 API
```
**설명** — 수용 기준: 정상 201 / 잔액 부족 409 `INSUFFICIENT_POINT` / 없는 메뉴·사용자 404.

**태스크**
1. `Order 엔티티 + OrderRepository 작성 (status, ordered_at)`
2. `OrderItem 엔티티 + OrderItemRepository 작성 (메뉴명·단가 스냅샷)`
3. `OrderCreateRequest DTO + 검증`
4. `OrderService.order 구현 (존재확인→총액→차감→주문/항목 저장, @Transactional)`
5. `OrderResponse DTO 작성 (잔액·상태 포함)`
6. `OrderController POST /api/orders 작성`
7. `단위/슬라이스 테스트 (정상/잔액부족/메뉴·사용자 없음)`

### └ Story E4-2 · 주문 이벤트 Kafka 전송 (필수 3번)
**제목**
```
주문 이벤트 Kafka 전송
```
**설명** — 수용 기준: 결제 완료 시 `order-events` 메시지 발행. (발행-트랜잭션 정합성은 E6)

**태스크**
1. `KafkaConfig/Producer 설정 (KafkaTemplate, order-events 토픽)`
2. `OrderEvent 메시지 스키마 정의 (orderId,userId,menuId,amount,orderedAt)`
3. `주문 결제 후 order-events 발행 로직`
4. `발행 검증 테스트 (EmbeddedKafka 또는 Mock)`

---

## [필수] EPIC E5 — 인기 메뉴

**제목**
```
[필수] 인기 메뉴
```
**설명**
```
최근 7일 인기 메뉴 조회.
```

### └ Story E5-1 · 인기 메뉴 조회 API
**제목**
```
인기 메뉴 조회 API
```
**설명** — 수용 기준: 주문 횟수 정확, 최근 7일 경계·동점(menu_id asc) 처리, top3.

**태스크**
1. `인기 메뉴 집계 쿼리 작성 (7일·PAID·수량합·동점 menu_id asc·top3)`
2. `PopularMenuResponse DTO 작성`
3. `PopularMenuService + Controller GET /api/menus/popular`
4. `집계 정확성 테스트 (7일 경계, 동점 정렬)`

---

## [도전] EPIC E6 — 도전 요구사항

**제목**
```
[도전] 도전 요구사항
```
**설명**
```
필수 구현 위에 얹는 레이어: 다수 인스턴스 안전성 · 동시성 · 데이터 일관성 · 테스트.
```

### └ Story E6-1 · ① 다수 인스턴스 무상태 설계
**제목**
```
① 다수 인스턴스 무상태 설계
```
**설명** — 수용 기준: 인스턴스 증설이 기능에 영향 없음을 근거로 설명·시연.

**태스크**
1. `세션/로컬 캐시 사용처 점검 및 제거`
2. `상태 외부화 확인 (잔액=MySQL, 락=Redis, 스트림=Kafka)`
3. `다중 인스턴스 기동 시나리오 문서화 (docker-compose scale)`

### └ Story E6-2 · ② 동시성 제어
**제목**
```
② 동시성 제어 (비관적 락 + Redis 분산락)
```
**설명** — 수용 기준: 동일 사용자 동시 충전/결제 N건 → 분실 갱신 없음(최종 잔액=기대값).

**태스크**
1. `Redis 의존성 추가 + RedisConfig 작성`
2. `분산락 유틸 작성 (point:{userId}, 타임아웃/해제 보장)`
3. `PointBalance 비관적 락 조회 (@Lock PESSIMISTIC_WRITE)`
4. `충전·차감·주문 경로에 비관적 락 + 분산락 적용`
5. `동시성 테스트 (ExecutorService N스레드, 최종 잔액·이력 검증)`

### └ Story E6-3 · ③ 데이터 일관성
**제목**
```
③ 데이터 일관성
```
**설명** — 수용 기준: 결제 롤백 시 잔액 원복 + 이벤트 미발행.

**태스크**
1. `차감+주문 저장 트랜잭션 경계 재점검 (원자성 보장)`
2. `Kafka 발행을 @TransactionalEventListener(AFTER_COMMIT)로 이동`
3. `롤백 시 미발행 테스트 작성`
4. `(확장) Transactional Outbox 테이블 + 릴레이 설계/구현`

### └ Story E6-4 · ④ 기능 및 제약 테스트
**제목**
```
④ 기능 및 제약 테스트
```
**설명** — 수용 기준: 각 기능·제약 대응 테스트 존재, `./gradlew test` 통과.

**태스크**
1. `단위 테스트 (서비스 규칙: 충전·차감·잔액부족)`
2. `슬라이스 테스트 (@WebMvcTest: 상태코드·검증·에러코드)`
3. `통합 테스트 (Testcontainers: MySQL/Redis/Kafka)`
4. `동시성 테스트 (동시 충전·결제)`
5. `일관성/이벤트 테스트 (롤백 ↔ 미발행)`

---

## 등록 순서 요약

1. 에픽: E1 → E2 → E3 → E4 → E5 → E6
2. 각 에픽 하위 스토리 → 스토리 하위 태스크
3. 우선순위: E1 → (E2·E3) → E4 → E5 → **E6** (평가 핵심은 E6)

> 태스크 총계: 필수 43개(E1 16 · E2 5 · E3 7 · E4 11 · E5 4) + 도전 17개(E6 3·5·4·5) = 60개.
