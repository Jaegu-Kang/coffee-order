# 차감+주문 저장 트랜잭션 경계 재점검 (원자성 보장) — 데이터 일관성 (E6-3 태스크 1)

- **상태**: 확정 (Plan → Generate 반영 완료. 코드 리뷰 결과 기존 트랜잭션 경계는 이상 없어
  프로덕션 코드는 변경하지 않았고, `OrderService` Javadoc에 재점검 결과 한 줄만 추가.
  근거를 실증하는 통합 테스트 `OrderServiceAtomicityTest`를 신규 작성)
- **범위**: `OrderService.order()`의 "포인트 차감(비관적락+이력) → 주문/주문항목 저장"
  구간이 실제로 하나의 물리 트랜잭션으로 묶여 원자성(부분 실패 시 전체 롤백)을 보장하는지
  코드 리뷰와 통합 테스트로 재점검한다. Kafka 발행을 `@TransactionalEventListener
  (AFTER_COMMIT)`로 옮기는 작업(태스크 2)·롤백 시 미발행 테스트(태스크 3)·Transactional
  Outbox(태스크 4)는 이 문서의 범위 밖이며 별도 서브태스크에서 다룬다.

## 1. 요구사항·제약 재확인

- `docs/design/jira-manual.md`(238~249행, Story E6-3 "③ 데이터 일관성"): 수용 기준은
  "결제 롤백 시 잔액 원복 + 이벤트 미발행"이며, 태스크 1이 "차감+주문 저장 트랜잭션 경계
  재점검 (원자성 보장)"이다. 태스크 2~4(AFTER_COMMIT 전환, 미발행 테스트, Outbox)는 별도
  태스크로 이 문서에서 다루지 않는다.
- `docs/api/order.md`("처리(하나의 트랜잭션)" 1~6단계): 사용자·메뉴 존재 확인 → 총액
  계산 → 비관적 락 잔액 조회 → 잔액 차감 + `USE` 이력 기록 → `orders`/`order_items` 저장 →
  커밋 후 Kafka 발행. 6번("커밋 후" 발행)과 현재 코드(커밋 전 동기 발행)의 불일치는 이미
  인지된 별도 이슈(태스크 2 범위)이므로 이번 문서에서는 그대로 두고 손대지 않는다.
- `CLAUDE.md`: "계획에 없는 범위 확장(불필요한 리팩터링 등) 금지." 이번 작업은 코드
  리뷰와 테스트 추가, `OrderService` Javadoc 한 줄 보강만 수행한다.
- AC 정의(이번 문서 기준): "포인트 차감(잔액 갱신 + `USE` 이력 insert) 이후 `orders`/
  `order_items` 저장 중 어느 하나라도 실패하면, 잔액·이력·주문·주문항목이 모두 저장 전
  상태로 롤백된다."

## 2. 코드 리뷰 — 트랜잭션 경계 점검

대상: `src/main/java/com/coffeeorder/order/service/OrderService.java`,
`src/main/java/com/coffeeorder/common/lock/RedisDistributedLock.java`,
`src/main/java/com/coffeeorder/common/exception/BusinessException.java`.

| 점검 항목 | 결과 |
| --- | --- |
| (a) 클래스 레벨 `@Transactional(readOnly = true)`와 `order()`의 `@Transactional` 오버라이드 | 이상 없음. `OrderService`는 클래스 레벨에 `@Transactional(readOnly = true)`를 두고 쓰기 메서드인 `order()`에만 `@Transactional`을 별도로 붙여 오버라이드한다(`docs/code-convention.md` "서비스 클래스 기본값 readOnly, 쓰기 메서드에만 @Transactional" 규칙 준수). |
| (b) `redisDistributedLock.executeWithLock(...)` 람다가 별도 스레드/트랜잭션을 열지 않는지 | 이상 없음. `RedisDistributedLock#executeWithLock`은 `task.get()`을 호출 스레드에서 동기 실행하고 `try/finally`로 락만 해제할 뿐, `ExecutorService`나 `@Async`, `REQUIRES_NEW` 등으로 별도 스레드/트랜잭션을 열지 않는다. 즉 람다 내부의 `pointBalanceRepository.save(...)`/`pointHistoryRepository.save(...)`는 `order()`를 감싼 동일 트랜잭션 안에서 실행된다. |
| (c) `BusinessException`이 스프링 기본 rollback-for 대상인지 | 이상 없음. `BusinessException extends RuntimeException`이며 `@Transactional`의 기본 롤백 규칙(unchecked 예외 시 롤백)에 해당한다. 별도의 `rollbackFor`/`noRollbackFor` 지정이 없어 기본 규칙이 그대로 적용된다. |
| (d) 포인트 차감~주문 저장 사이에 트랜잭션 경계를 끊는 코드가 있는지 | 이상 없음. `order()` 메서드 전체(사용자·메뉴 확인 → 총액 계산 → 락+비관적락 하에서 차감/이력 저장 → `orders`/`order_items` 저장 → Kafka 발행)에 `@Transactional(propagation = Propagation.REQUIRES_NEW)` 등 새 트랜잭션을 여는 코드가 없다. `redisDistributedLock`·`pointBalanceRepository`·`pointHistoryRepository`·`orderRepository`·`orderItemRepository` 어디에도 트랜잭션 전파 속성을 바꾸는 애너테이션이 없다. |
| (e) self-invocation으로 프록시를 우회하는 내부 메서드 호출이 없는지 | 이상 없음. `order()`는 `this.xxx()` 형태로 같은 클래스의 다른 `@Transactional` 메서드를 호출하지 않는다(내부 로직은 람다와 리포지토리 호출뿐). |

**결론**: 5개 항목 모두 이상 없음. 포인트 차감(잔액 갱신 + `USE` 이력 insert)부터
`orders`/`order_items` 저장까지가 `order()` 단일 `@Transactional` 메서드(스프링 프록시가
관리하는 물리적으로 하나의 트랜잭션) 안에서 실행되며, 별도 트랜잭션 경계 없이 예외가
발생하면 스프링 기본 규칙에 따라 전체 롤백된다. 코드 변경은 필요하지 않았다.

## 3. 통합 테스트로 실증

신규 파일: `src/test/java/com/coffeeorder/order/service/OrderServiceAtomicityTest.java`.

- 방식: `OrderServiceConcurrencyTest`와 동일하게 `@DataJpaTest` + `@Import`로 실제 스프링
  `@Transactional` 프록시를 사용하고, `@Transactional(propagation = Propagation.NOT_SUPPORTED)`
  로 테스트 메서드 자동 롤백을 끈 뒤 `TransactionTemplate`으로 시드 데이터 커밋을 격리한다
  (`@DataJpaTest` 기본 롤백과 "검증하려는 트랜잭션 롤백"을 혼동해 거짓 양성이 나오는 것을
  방지).
- 강제 실패 유도: 테스트 전용 `@Primary` `OrderItemRepository` 빈을 JDK 동적 프록시로
  등록해, 실제 리포지토리에는 위임하되 `save(OrderItem)` 호출만 가로채 `RuntimeException`을
  던진다. 프로덕션 `OrderService`/`OrderItemRepository` 코드는 변경하지 않는다.
- 시나리오 1(`기존_사용자의_주문항목_저장이_실패하면_잔액_차감과_포인트이력과_주문이_모두_롤백된다`):
  기존 잔액이 있는 사용자가 주문 → `order_items` 저장 단계에서 강제 실패 → `order()`가
  예외를 던지고, 잔액은 차감 전 값 그대로, `USE` 이력 없음, `orders`에 해당 행 없음,
  `order_items` 총 개수 불변, `kafkaTemplate.send`가 호출되지 않음(`verify(never())`)을
  모두 확인.
- 시나리오 2(`신규_사용자는_주문항목_저장이_실패하면_잔액행_INSERT_자체가_롤백되어_잔액행이_남지_않는다`):
  `point_balances` 행이 아예 없는 신규 사용자의 첫 주문에서 잔액 행 **INSERT** 자체가
  롤백되는지 확인(빠뜨리기 쉬운 엣지 케이스). 현재 구현상 잔액이 없는 사용자는 기본 잔액
  0으로 취급되어 총액이 0보다 크면 잔액 검사에서 먼저 `INSUFFICIENT_POINT`로 실패해 INSERT
  분기(`orElseGet`)까지 도달하지 못하므로, `docs/db/schema.md`의 `menus.price CHECK(price
  >= 0)`로 허용되는 가격 0원짜리 테스트 전용 메뉴를 사용해 잔액 검사를 통과시키고 강제 실패
  경로까지 도달시킨다. 결과: `order()`가 예외를 던지고, `point_balances`에 해당 사용자 행이
  전혀 없고(INSERT 자체가 롤백), `USE` 이력 없음, `orders`에 해당 행 없음,
  `kafkaTemplate.send`가 호출되지 않음을 확인.
- 실행 결과: `./gradlew test --tests
  "com.coffeeorder.order.service.OrderServiceAtomicityTest"` 통과(2개 테스트 모두 green).

## 4. 회귀 확인

- `./gradlew test` 전체 실행 결과 그린. 기존 `OrderServiceTest`(Mockito 단위),
  `OrderServiceConcurrencyTest`(SCRUM-73/74 동시성), `PointServiceConcurrencyTest` 등이
  이번 변경(테스트 신규 추가 + `OrderService` Javadoc 보강)으로 깨지지 않음을 확인했다.
  프로덕션 로직을 변경하지 않았으므로 동시성 테스트의 락 순서·타이밍에도 영향이 없다.

## 5. 결론 및 후속 태스크와의 범위 경계

- 결론: `OrderService.order()`는 포인트 차감(잔액 갱신 + `USE` 이력 insert)부터 `orders`/
  `order_items` 저장까지 하나의 물리 트랜잭션으로 처리되며, 어느 단계든 실패하면(기존
  사용자의 잔액 UPDATE, 신규 사용자의 잔액 행 INSERT 모두 포함) 전체 롤백된다. AC("결제
  롤백 시 잔액 원복")는 이 시점 기준으로 이미 충족된다.
- 이벤트 미발행 부분(AC의 나머지 절반)은 현재 구현(커밋 전 동기 발행)에서도 예외 발생 시
  `kafkaTemplate.send(...)`에 도달하지 못해 자연히 발행되지 않음을 3절 테스트에서 함께
  확인했다. 다만 "커밋 후에만 발행"이라는 `docs/api/order.md` 6번 항목과의 완전한 정합(트랜잭션
  커밋은 성공했지만 이후 다른 이유로 후속 처리가 실패하는 경우 등)은 `@TransactionalEventListener
  (AFTER_COMMIT)` 전환(태스크 2)과 그 전용 테스트(태스크 3)에서 별도로 다룬다.
- Transactional Outbox(태스크 4)는 발행 자체의 신뢰성 강화(브로커 장애 시 재전송 등) 옵션이며
  이번 문서·테스트가 다루는 "DB 트랜잭션 원자성"과는 다른 층위의 문제이므로 범위 밖이다.

## 참조

- `docs/design/jira-manual.md`(238~249행, Story E6-3), `docs/api/order.md`(처리 1~6단계),
  `docs/policy.md`, `docs/db/schema.md`(`menus.price CHECK(price >= 0)`),
  `src/main/java/com/coffeeorder/order/service/OrderService.java`,
  `src/main/java/com/coffeeorder/common/lock/RedisDistributedLock.java`,
  `src/main/java/com/coffeeorder/common/exception/BusinessException.java`,
  `src/test/java/com/coffeeorder/order/service/OrderServiceAtomicityTest.java`,
  `src/test/java/com/coffeeorder/order/service/OrderServiceConcurrencyTest.java`(패턴 재사용),
  `docs/design/state-externalization-check.md`/`docs/design/session-cache-check.md`/
  `docs/design/multi-instance-scale-check.md`(문서 형식 참고)
