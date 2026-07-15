# 설계 문서 — 커피숍 주문 시스템 (0번 Plan 산출물)

- **목표**: 다중 인스턴스 환경에서 정합성 있게 동작하는 커피 주문/결제/추천 시스템 설계 확정.
- **범위(포함)**: 메뉴 조회, 포인트 충전, 주문/결제(+Kafka 전송), 인기 메뉴(7일). 동시성·일관성·확장성 전략.
- **범위(제외)**: 회원가입/인증(시드로 대체), 관리자 기능, 실제 결제 PG 연동.

## 계층 설계 (구현 시)

```
com.myharness
├── menu     (controller/service/repository/entity/dto)
├── point    (+ 분산락, 비관적 락 repository)
├── order    (+ Kafka producer, TransactionalEventListener)
├── popular  (집계 조회)
├── common   (ErrorResponse, GlobalExceptionHandler, 에러코드 상수, 분산락 유틸)
└── config   (JPA/Redis/Kafka 설정, JpaAuditingConfig)
```

## 성공 기준 (Evaluate가 판정)

- 4개 API가 명세(`docs/api/`)의 계약(상태코드/에러코드/응답형식)과 일치.
- 동일 사용자 동시 충전/결제 테스트에서 **분실 갱신 없음**(최종 잔액 = 산술 기대값).
- 결제 롤백 시 잔액 원복 + Kafka 미발행.
- 인기 메뉴가 최근 7일·수량합·동점 규칙대로 정확.
- 컨벤션(`docs/code-convention.md`) 준수: 계층분리·생성자주입·트랜잭션·엔티티 미노출·Lombok 미사용.

## 참조

- DB: `docs/db/schema.md`
- API: `docs/api/menu.md`, `docs/api/point.md`, `docs/api/order.md`
- 백로그: `docs/design/jira-backlog.md`
- 제출 문서: `README.md`

## 리스크

- Kafka/Redis 도입으로 로컬 구동 복잡도 증가 → docker-compose로 완화.
- 비관적 락 사용 시 락 대기·데드락 → 잠금 순서 고정, 트랜잭션 짧게 유지, 분산락 타임아웃 설정.
- AFTER_COMMIT 발행의 유실 가능성 → 신뢰성 요구 시 Transactional Outbox로 확장.
