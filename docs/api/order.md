# API 명세 — 주문/결제

## POST /api/orders — 커피 주문 및 결제

- 설명: 사용자 식별값과 메뉴 ID(수량)를 받아 주문을 생성하고 포인트로 결제.
- 요청 바디

```json
{ "userId": 1, "menuId": 2, "quantity": 1 }
```

- 검증: `userId` `@NotNull`, `menuId` `@NotNull`, `quantity` `@Positive`(기본 1).
- 처리(하나의 트랜잭션):
  1. 사용자·메뉴 존재 확인 (없으면 404).
  2. 결제 총액 = `menu.price * quantity` 계산.
  3. `point_balances`를 **비관적 락**으로 조회 → 잔액 부족이면 `INSUFFICIENT_POINT`.
  4. 잔액 차감 + `point_histories(USE)` 기록.
  5. `orders(status=PAID)` + `order_items`(메뉴명/단가 스냅샷) 저장.
  6. 커밋 **후**(AFTER_COMMIT) 주문 이벤트를 Kafka 토픽 `order-events`로 발행.
- 다중 인스턴스 동시 주문: Redis 분산락(key=`point:{userId}`)으로 동일 사용자 요청 직렬화.
- 응답 201

```json
{
  "orderId": 1001,
  "userId": 1,
  "items": [
    { "menuId": 2, "name": "카페라떼", "unitPrice": 3500, "quantity": 1 }
  ],
  "totalAmount": 3500,
  "balanceAfter": 21500,
  "status": "PAID"
}
```

- 에러
  - `USER_NOT_FOUND` (404), `MENU_NOT_FOUND` (404)
  - `INSUFFICIENT_POINT` (409) — 잔액 부족
  - `VALIDATION_ERROR` (400)
  - `CONCURRENCY_CONFLICT` (409) — 락 획득 실패/낙관적 충돌(재시도 후에도 실패 시)

## Kafka 이벤트 — 토픽 `order-events`

주문 결제 완료 시 데이터 수집 플랫폼으로 전송하는 메시지(요구사항 3의 실시간 전송).

```json
{
  "orderId": 1001,
  "userId": 1,
  "menuId": 2,
  "amount": 3500,
  "orderedAt": "2026-07-13T10:00:00Z"
}
```

- 발행 시점: 결제 트랜잭션 커밋 이후(AFTER_COMMIT) — 결제 실패 건은 전송하지 않음(일관성).
- 전송 보장 강화 옵션: Transactional Outbox(아웃박스 테이블 + 릴레이) — README 참조.
