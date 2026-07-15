# API 명세 — 포인트

## POST /api/points/charge — 포인트 충전

- 설명: 사용자 식별값과 충전 금액을 받아 포인트를 충전(1원 = 1P).
- 요청 바디

```json
{ "userId": 1, "amount": 10000 }
```

- 검증: `userId` `@NotNull`, `amount` `@NotNull` + 양수(`@Positive`). (선택) 1회 최대 충전 한도.
- 처리: 사용자 존재 확인 → `point_balances` 행을 **비관적 락**으로 조회 →
  잔액 증가 → `point_histories(CHARGE)` 기록. 다중 인스턴스 동시 충전은 Redis 분산락(key=`point:{userId}`)로 직렬화.
- 응답 200

```json
{ "userId": 1, "balance": 25000 }
```

- 에러
  - `USER_NOT_FOUND` (404)
  - `INVALID_AMOUNT` (400) — 0 이하
  - `VALIDATION_ERROR` (400) — 필드 누락/형식

## (참고) 잔액 조회 — GET /api/points/{userId}

- 과제 필수는 아니나 검증 편의를 위해 제공 가능. 응답 `{ "userId":1, "balance":25000 }`.
