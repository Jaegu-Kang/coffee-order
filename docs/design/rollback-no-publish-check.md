# 롤백 시 미발행 테스트 — 데이터 일관성 (E6-3 태스크 3, SCRUM-77)

- **상태**: 확정 (Plan → Generate 반영 완료. 프로덕션 코드는 전혀 수정하지 않고, 신규 통합
  테스트 `OrderServiceAfterCommitPublishTest`만 추가했다.)
- **범위**: `OrderService.order()`가 실제 스프링 트랜잭션 커밋/롤백과
  `OrderEventKafkaListener`(`@TransactionalEventListener(phase = AFTER_COMMIT)`)를 함께 사용할
  때 "커밋 성공 시에만 Kafka 발행되고, 롤백 시에는 구조적으로 발행되지 않음"을 실제 리스너
  배선(wiring)까지 포함해 통합 테스트로 실증한다(SCRUM-22 AC "결제 롤백 시 잔액 원복 + 이벤트
  미발행"의 나머지 절반). Transactional Outbox(태스크 4)는 이 문서의 범위 밖이며 별도
  서브태스크에서 다룬다.

## 1. 요구사항·제약 재확인

- `docs/design/jira-backlog.md`(74~78행, Story E6-3 "③ 데이터 일관성"): AC는 "결제 롤백 시
  잔액 원복 + 이벤트 미발행"이며, 태스크 3이 "일관성(롤백↔미발행) 테스트"다.
- `docs/design/kafka-after-commit-check.md`(6절 "태스크 3(롤백 시 미발행 테스트)·태스크
  4(Outbox)와의 범위 경계"): 태스크 2가 만든 것은 "커밋 후에만 Kafka로 발행되는 코드 경로"
  까지이고, "커밋 성공 시 실제로 커밋 이후에만 발행되는지"·"강제 롤백 시 리스너 자체가
  호출되지 않는지"를 실제 리스너 배선까지 포함해 직접 실증하는 통합 테스트는 의도적으로 이
  태스크 3 범위로 남겨두었다고 명시한다. 이번 문서는 그 갭을 메운다.
- `docs/design/transaction-atomicity-check.md`(태스크 1): `OrderServiceAtomicityTest`가 이미
  잔액 원복(AC 전반부)과 `kafkaTemplate.send` never-called를 검증하지만,
  `OrderEventKafkaListener`를 `@Import`하지 않아 "예외가 `eventPublisher.publishEvent()` 호출
  전에 터져 애초에 이벤트가 안 만들어짐"만 증명할 뿐 AFTER_COMMIT 리스너 자체가 롤백 시
  호출되지 않는지는 검증하지 못한다는 한계가 같은 문서(3절)에 기록되어 있다.
- `CLAUDE.md`: "계획에 없는 범위 확장(불필요한 리팩터링 등) 금지." 이번 작업은 신규 통합
  테스트 1개 클래스(+ 이 문서) 추가만 수행하며, `OrderService`/`OrderEventKafkaListener`/
  `OrderEvent`/`KafkaProducerConfig` 등 프로덕션 코드는 일절 수정하지 않는다.

## 2. 기존 테스트와의 실제 갭

| 테스트 | `OrderEventKafkaListener` `@Import` 여부 | 무엇을 증명하는가 |
| --- | --- | --- |
| `OrderServiceAtomicityTest`(태스크1) | 안 함 | 저장 단계 실패 시 `eventPublisher.publishEvent()` 호출 지점까지 코드가 도달하지 못함(발행 "트리거" 자체가 안 만들어짐) |
| `OrderEventKafkaListenerTest` | 해당 없음(리스너를 Mockito로 직접 생성) | 리스너 메서드에 이벤트를 직접 전달하면 `kafkaTemplate.send(...)`가 호출됨(트랜잭션 인프라는 검증 범위 밖으로 명시적으로 제외) |
| `KafkaProducerConfigEmbeddedKafkaTest` | 해당 없음(`OrderEvent`를 수동 생성해 `kafkaTemplate.send` 직접 호출) | `OrderEvent` 직렬화·역직렬화 계약(실제 Kafka 브로커 경유) — `OrderService`/트랜잭션/리스너 경로 전혀 안 거침 |
| `OrderServiceAfterCommitPublishTest`(신규, 태스크3) | **함** | `OrderService.order()` 호출이 실제 커밋되면 `OrderEventKafkaListener`가 실제로 호출되어 발행되고, 실제 롤백되면 호출되지 않음 |

세 기존 테스트 중 어느 것도 "`OrderService.order()`가 실제 스프링 트랜잭션 경계 안에서 실행되고,
그 커밋/롤백 결과에 따라 실제로 등록된 `AFTER_COMMIT` 리스너가 호출되는지 안 되는지"를 함께
검증하지 않는다. 이것이 이번 SCRUM-77의 범위이자 신규 테스트가 메우는 갭이다.

## 3. 통합 테스트로 실증

신규 파일: `src/test/java/com/coffeeorder/order/service/OrderServiceAfterCommitPublishTest.java`.

- 방식: `OrderServiceAtomicityTest`/`OrderServiceConcurrencyTest`와 동일하게 `@DataJpaTest` +
  `@Import`로 실제 스프링 `@Transactional` 프록시를 사용하고,
  `@Transactional(propagation = Propagation.NOT_SUPPORTED)`로 테스트 메서드 자동 롤백을 끈 뒤
  `TransactionTemplate`으로 시드 데이터 커밋/정리를 격리한다(`@DataJpaTest` 기본 롤백과
  "검증하려는 트랜잭션 커밋/롤백"을 혼동해 거짓 양성이 나오는 것을 방지 — 두 선행 테스트가 이미
  문서화해둔 함정).
- **핵심 차이점**: `@Import` 목록에 `OrderEventKafkaListener.class`를 반드시 포함해 AFTER_COMMIT
  리스너가 실제 스프링 트랜잭션 이벤트 버스에 등록되게 했다. `OrderServiceAtomicityTest`는
  이 클래스를 의도적으로 `@Import`하지 않았으므로(1·2절 참고), 이번 테스트가 그 갭을 메우는
  지점이다.
- 강제 실패 유도: `OrderServiceAtomicityTest`와 동일하게 테스트 전용 `@Primary`
  `OrderItemRepository` 빈을 JDK 동적 프록시로 등록해 `save(OrderItem)` 호출만 가로채
  `RuntimeException`을 던진다. 다만 시나리오 A(커밋 성공)/B(롤백)를 한 클래스(스프링 컨텍스트
  캐싱 재사용) 안에서 함께 다루기 위해, 강제 실패 여부를 `ForcedFailureToggle`
  (`AtomicBoolean` 기반)로 테스트 메서드별로 켜고 끌 수 있게 했고, `@AfterEach`
  (`resetForcedFailureAndMock`)에서 토글 리셋과 `Mockito.reset(kafkaTemplate)`을 함께 수행해
  테스트 간 상태 누수(오탐)를 방지했다. 프로덕션 `OrderService`/`OrderItemRepository` 코드는
  변경하지 않는다.
- 시나리오 A(`커밋되면_AFTER_COMMIT_리스너가_실제로_호출되어_Kafka로_정확히_1회_발행된다`):
  충분한 잔액을 가진 사용자로 정상 `orderService.order(...)` 호출(강제 실패 미적용) →
  정상 반환 확인 후, `verify(kafkaTemplate, times(1)).send(eq(ORDER_EVENTS_TOPIC),
  eventCaptor.capture())`로 정확히 1회 호출을 확인하고, 캡처된 `OrderEvent`의
  `orderId/userId/menuId/amount/orderedAt`이 실제 저장된 `Order`/`OrderItem`(DB에서 다시 조회)과
  모두 일치함을 확인한다.
- 시나리오 B(`저장_단계에서_실패해_롤백되면_AFTER_COMMIT_리스너가_호출되지_않아_Kafka로_발행되지_않는다`,
  이 티켓의 핵심): `forcedFailureToggle.enable()`로 `order_items` 저장 단계에서 강제 실패를
  유도한 뒤 `orderService.order(...)`가 예외를 던짐을 확인하고,
  `verify(kafkaTemplate, never()).send(any(), any())`로 AFTER_COMMIT 리스너 자체가 호출되지
  않았음을 확인한다. `OrderEventKafkaListener`가 실제로 컨텍스트에 등록된 상태에서 이 결과를
  얻었다는 점이 `OrderServiceAtomicityTest`(리스너 미등록)와의 결정적 차이다. 잔액 원복은 이미
  `OrderServiceAtomicityTest`가 커버하므로 이 테스트에서는 중복 검증하지 않고 "이벤트 미발행 +
  리스너 배선 실증"에 집중했다.
- 실행 결과: `./gradlew test --tests
  "com.coffeeorder.order.service.OrderServiceAfterCommitPublishTest"` 통과(2개 테스트 모두
  green).

## 4. 회귀 확인

- `./gradlew test` 전체 실행 결과 전 테스트 클래스 green(`failures="0" errors="0"`), 특히
  `OrderServiceAtomicityTest`, `OrderServiceConcurrencyTest`, `OrderEventKafkaListenerTest`,
  `KafkaProducerConfigEmbeddedKafkaTest`가 이번 변경(신규 테스트 클래스 추가만)으로 깨지지
  않음을 확인했다. 프로덕션 로직을 전혀 변경하지 않았으므로 기존 테스트의 검증 대상 동작
  자체에는 영향이 없다.

## 5. 영향받은 파일 목록

- 테스트(신규)
  - `src/test/java/com/coffeeorder/order/service/OrderServiceAfterCommitPublishTest.java`:
    실제 `OrderEventKafkaListener`를 `@Import`해 AFTER_COMMIT 배선을 실제로 등록한 상태에서
    커밋 성공 시 1회 발행 / 강제 롤백 시 미발행 두 시나리오를 검증.
- 문서(신규)
  - `docs/design/rollback-no-publish-check.md`(이 문서).
- 프로덕션 코드: 변경 없음(`OrderService`, `OrderEventKafkaListener`, `OrderEvent`,
  `KafkaProducerConfig` 모두 무수정).

## 6. 후속 태스크(Outbox, 태스크 4)와의 범위 경계

- 이번 작업은 "실제 리스너 배선까지 포함해 커밋→발행/롤백→미발행 구조를 통합 테스트로
  실증"하는 것까지가 범위다. 동기 `AFTER_COMMIT` 리스너의 신뢰성 한계 — 커밋이 이미 끝난
  시점에서 리스너가 실행되므로 리스너 내부(예: Kafka 브로커 장애로 인한 `send` 실패) 실패는
  DB 트랜잭션 롤백을 유발하지 못하고, 발행 실패에 대한 재시도/영속 큐도 없다 — 는 이번 문서·
  테스트가 다루는 "리스너가 커밋/롤백에 맞춰 호출되는지"와는 다른 층위의 문제이므로 범위 밖이다.
  이를 근본적으로 보완하는 Transactional Outbox 패턴 도입은 태스크 4(`docs/design/jira-backlog.md`
  77행 "(확장) Transactional Outbox 패턴 설계/구현")에서 별도로 다룬다.
- 이번 신규 테스트는 리스너 메서드 내부에서 예외가 발생하는 케이스(예: `kafkaTemplate.send`
  자체가 실패하는 경우)는 다루지 않는다. 이는 태스크 4의 Outbox 설계와 맞물린 별도 시나리오이며,
  현재 문서·테스트는 "정상 리스너 호출 여부(커밋 시 호출/롤백 시 미호출)"만 검증한다.

## 참조

- `docs/design/jira-backlog.md`(74~78행, Story E6-3), `docs/design/kafka-after-commit-check.md`
  (6절 "태스크3과의 범위 경계", 이번 작업이 이어받는 부분),
  `docs/design/transaction-atomicity-check.md`(태스크1, 패턴 재사용 대상),
  `docs/api/order.md`(처리 6단계 "커밋 후 AFTER_COMMIT 발행", "Kafka 이벤트" 절),
  `src/main/java/com/coffeeorder/order/service/OrderService.java`,
  `src/main/java/com/coffeeorder/order/event/OrderEventKafkaListener.java`,
  `src/main/java/com/coffeeorder/order/event/OrderEvent.java`,
  `src/main/java/com/coffeeorder/config/KafkaProducerConfig.java`,
  `src/test/java/com/coffeeorder/order/service/OrderServiceAtomicityTest.java`,
  `src/test/java/com/coffeeorder/order/service/OrderServiceConcurrencyTest.java`(패턴 재사용),
  `src/test/java/com/coffeeorder/order/event/OrderEventKafkaListenerTest.java`(역할 구분),
  `src/test/java/com/coffeeorder/config/KafkaProducerConfigEmbeddedKafkaTest.java`(역할 구분),
  `src/test/java/com/coffeeorder/common/lock/InMemoryRedisDistributedLockTestConfig.java`,
  `src/main/java/com/coffeeorder/config/JpaAuditingConfig.java`,
  `src/test/java/com/coffeeorder/order/service/OrderServiceAfterCommitPublishTest.java`(신규)
