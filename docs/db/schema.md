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

## 인덱스 요약

- `point_histories(user_id, created_at)` — 사용자 이력 조회.
- `orders(ordered_at)` — 최근 7일 인기 메뉴 집계.
- `order_items(menu_id)`, `order_items(order_id)` — 집계/조인.
