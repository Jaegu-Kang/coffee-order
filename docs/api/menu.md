# API 명세 — 메뉴

## GET /api/menus — 커피 메뉴 목록 조회

- 인증: 없음(과제 범위)
- 요청: 없음
- 응답 200

```json
{
  "menus": [
    { "id": 1, "name": "아메리카노", "price": 3000 },
    { "id": 2, "name": "카페라떼", "price": 3500 }
  ]
}
```

## GET /api/menus/popular — 인기 메뉴 목록 조회

- 설명: **최근 7일**(호출 시각 기준 168시간) 결제 완료(`orders.status = PAID`) 주문을
  대상으로 메뉴별 주문 횟수 상위 3개를 반환.
- "주문 횟수" 정의: 해당 메뉴가 포함된 `order_items`의 `quantity` 합. (수량 반영)
- 동점 처리: 주문 횟수 desc, 동점 시 `menu_id` asc로 안정 정렬.
- 요청: 없음
- 응답 200

```json
{
  "popularMenus": [
    { "menuId": 1, "name": "아메리카노", "price": 3000, "orderCount": 42 },
    { "menuId": 3, "name": "바닐라라떼", "price": 4000, "orderCount": 31 },
    { "menuId": 2, "name": "카페라떼", "price": 3500, "orderCount": 20 }
  ]
}
```

- 집계 쿼리(개념):
  `SELECT oi.menu_id, SUM(oi.quantity) cnt FROM order_items oi JOIN orders o ON o.id=oi.order_id
   WHERE o.status='PAID' AND o.ordered_at >= :from GROUP BY oi.menu_id ORDER BY cnt DESC, oi.menu_id ASC LIMIT 3`
