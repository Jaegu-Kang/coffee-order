# DB 명세 — 커피숍 주문 시스템

운영 DB는 MySQL, 테스트는 H2(MySQL 호환 모드). 모든 테이블은 `created_at`/`updated_at` 감사 컬럼을 가진다.
금액·포인트는 `BIGINT`(원 단위, 1원=1P), 소수점 없음.

## users — 사용자

| 컬럼 | 타입 | NULL | 기타 |
| --- | --- | --- | --- |
| id | BIGINT | N | PK, AUTO_INCREMENT |
| name | VARCHAR(100) | N | 사용자명 |
| created_at | DATETIME(6) | N | |
| updated_at | DATETIME(6) | N | |

> 사용자 식별값 = `users.id`. 과제 단순화를 위해 회원가입 API는 범위 밖(시드 데이터로 제공).

## point_balances — 포인트 잔액 (동시성 제어 대상)

| 컬럼 | 타입 | NULL | 기타 |
| --- | --- | --- | --- |
| user_id | BIGINT | N | PK, FK→users.id |
| balance | BIGINT | N | 현재 잔액(P), DEFAULT 0, CHECK(balance >= 0) |
| version | BIGINT | N | 낙관적 락 병행용, DEFAULT 0 |
| updated_at | DATETIME(6) | N | |

> 잔액을 users와 분리한 이유: 충전/차감 시 **행 단위 비관적 락**(`SELECT ... FOR UPDATE`)을
> 잔액 행에만 걸어 잠금 범위를 최소화하기 위함. `version`은 방어적 낙관적 락 병행용.

## point_histories — 포인트 이력

| 컬럼 | 타입 | NULL | 기타 |
| --- | --- | --- | --- |
| id | BIGINT | N | PK, AUTO_INCREMENT |
| user_id | BIGINT | N | FK→users.id, INDEX(user_id, created_at) |
| type | VARCHAR(10) | N | CHARGE / USE |
| amount | BIGINT | N | 변동액(양수) |
| balance_after | BIGINT | N | 처리 후 잔액(감사·정합성 검증용) |
| created_at | DATETIME(6) | N | |

## menus — 메뉴

| 컬럼 | 타입 | NULL | 기타 |
| --- | --- | --- | --- |
| id | BIGINT | N | PK, AUTO_INCREMENT |
| name | VARCHAR(100) | N | 메뉴명 |
| price | BIGINT | N | 가격(P), CHECK(price >= 0) |
| created_at | DATETIME(6) | N | |
| updated_at | DATETIME(6) | N | |

## orders — 주문(결제 단위)

| 컬럼 | 타입 | NULL | 기타 |
| --- | --- | --- | --- |
| id | BIGINT | N | PK, AUTO_INCREMENT |
| user_id | BIGINT | N | FK→users.id |
| total_amount | BIGINT | N | 결제 총액(P) |
| status | VARCHAR(20) | N | PAID (결제 성공만 저장), 확장 시 CANCELED 등 |
| ordered_at | DATETIME(6) | N | 주문 시각, INDEX(ordered_at) — 인기 메뉴 집계용 |
| created_at | DATETIME(6) | N | |

## order_items — 주문 항목 (메뉴 스냅샷)

| 컬럼 | 타입 | NULL | 기타 |
| --- | --- | --- | --- |
| id | BIGINT | N | PK, AUTO_INCREMENT |
| order_id | BIGINT | N | FK→orders.id, INDEX(order_id) |
| menu_id | BIGINT | N | FK→menus.id, INDEX(menu_id) |
| menu_name | VARCHAR(100) | N | 주문 시점 메뉴명 스냅샷 |
| unit_price | BIGINT | N | 주문 시점 단가 스냅샷 |
| quantity | INT | N | 수량, DEFAULT 1, CHECK(quantity >= 1) |

> 메뉴명·단가를 스냅샷으로 저장하는 이유: 이후 메뉴 가격이 바뀌어도 과거 주문 금액·이력이
> 불변이어야 하기 때문(데이터 일관성). 인기 메뉴 집계는 `menu_id` 기준.

## outbox_events — 발행 이벤트 아웃박스 (E6-3 태스크4, SCRUM-78 확장)

| 컬럼 | 타입 | NULL | 기타 |
| --- | --- | --- | --- |
| id | BIGINT | N | PK, AUTO_INCREMENT |
| aggregate_type | VARCHAR(50) | N | 발행 대상 애그리게잇 유형(예: `ORDER`) |
| aggregate_id | BIGINT | N | 발행 대상 애그리게잇 식별자(예: `orders.id`) |
| topic | VARCHAR(100) | N | 발행될 Kafka 토픽명(예: `order-events`) |
| payload | VARCHAR(4000) | N | 발행할 메시지 본문(JSON 직렬화 문자열) |
| status | VARCHAR(20) | N | PENDING / SENT / FAILED, DEFAULT `PENDING` |
| retry_count | INT | N | 발행 재시도 횟수, DEFAULT 0 |
| created_at | DATETIME(6) | N | INDEX(status, created_at) — 릴레이 폴링용 |
| updated_at | DATETIME(6) | N | |
| sent_at | DATETIME(6) | Y | 실제 발행 성공 시각(성공 전에는 NULL) |

> `OrderService.order()`가 포인트 차감·주문/주문항목 저장과 **같은 물리 트랜잭션**에서 이 테이블에
> `PENDING` 행을 원자적으로 기록한다(Transactional Outbox 패턴). 실제 Kafka 발행은 별도
> `OutboxRelay`가 이 테이블을 폴링해 담당하며, 트랜잭션 내부에서 직접 브로커로 전송하지 않는다
> (docs/design/outbox-relay-design.md). 컨슈머 측 멱등 처리·행 보관/정리(retention) 정책은 이
> 확장 범위 밖이다(at-least-once 전달만 보장).

## 인덱스 요약

- `point_histories(user_id, created_at)` — 사용자 이력 조회.
- `orders(ordered_at)` — 최근 7일 인기 메뉴 집계.
- `order_items(menu_id)`, `order_items(order_id)` — 집계/조인.
- `outbox_events(status, created_at)` — Outbox 릴레이 폴링(PENDING 오래된 순).
