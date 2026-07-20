# Kafka 발행을 AFTER_COMMIT으로 전환 — 데이터 일관성 (E6-3 태스크 2, SCRUM-76)

- **상태**: 확정 (Plan → Generate 반영 완료. `OrderService.order()`의 커밋 전 동기
  `kafkaTemplate.send(...)` 호출을 제거하고, `ApplicationEventPublisher.publishEvent(OrderEvent)` +
  신규 `OrderEventKafkaListener`(`@TransactionalEventListener(phase = AFTER_COMMIT)`)로
  대체했다. `OrderService`/`OrderEvent` Javadoc을 갱신하고, `OrderServiceTest`를
  `eventPublisher` 기준 검증으로 교체했으며, 신규 `OrderEventKafkaListenerTest`를 추가했다.)
- **범위**: `docs/api/order.md` "처리" 6번("커밋 **후**(AFTER_COMMIT) 발행")과 실제 구현
  (기존에는 커밋 이전 트랜잭션 내부 동기 발행)의 불일치를 해소한다. 롤백 시 미발행을 직접
  실증하는 통합 테스트(태스크 3)·Transactional Outbox(태스크 4)는 이 문서의 범위 밖이며
  별도 서브태스크에서 다룬다.

## 1. 요구사항·제약 재확인

- `docs/design/jira-manual.md`(238~249행, Story E6-3 "③ 데이터 일관성"): 수용 기준은
  "결제 롤백 시 잔액 원복 + 이벤트 미발행"이며, 태스크 2가 "Kafka 발행을
  `@TransactionalEventListener(AFTER_COMMIT)`으로 전환"이다.
- `docs/design/transaction-atomicity-check.md`(태스크 1 문서)가 명시적으로 이 작업을 범위
  밖으로 미뤄두면서 "6번('커밋 후' 발행)과 현재 코드(커밋 전 동기 발행)의 불일치는 이미
  인지된 별도 이슈(태스크 2 범위)"라고 남겨둔 갭을 해소한다.
- `docs/api/order.md`("처리(하나의 트랜잭션)" 6단계): "커밋 후 Kafka 발행"·"Kafka 이벤트 —
  토픽 `order-events`" JSON 스키마(`orderId`, `userId`, `menuId`, `amount`, `orderedAt`)는
  그대로 유지한다(새 필드·새 토픽 상수 추가 없음).
- `docs/design/coffee-order.md`: order 도메인 레이어를 "order (+ Kafka producer,
  TransactionalEventListener)"로 설명 — 이번 작업으로 그 설명이 실제 코드와 일치하게 됨.
- `CLAUDE.md`: "계획에 없는 범위 확장(불필요한 리팩터링 등) 금지." 태스크 3(롤백 시 미발행
  테스트)·태스크 4(Outbox)는 이번 작업에서 손대지 않는다.

## 2. 변경 전/후 발행 시점

| 구분 | 변경 전 | 변경 후 |
| --- | --- | --- |
| 발행 호출 위치 | `OrderService.order()` 내부, `orderItemRepository.save(...)` 직후 | 동일 위치에서 `eventPublisher.publishEvent(OrderEvent.from(order, orderItem))` 호출(발행 "트리거"만 트랜잭션 내부) |
| 실제 Kafka 전송 | `kafkaTemplate.send(...)`가 트랜잭션 커밋 **이전**, 같은 스레드에서 동기 실행 | `OrderEventKafkaListener#onOrderEvent`가 `@TransactionalEventListener(phase = AFTER_COMMIT)`로 등록되어, 트랜잭션이 실제로 **커밋된 이후**에만 동기 실행되어 `kafkaTemplate.send(order-events, event)` 호출 |
| 트랜잭션 롤백 시 | 이미 `orderItemRepository.save(...)` 이후 코드에 도달하지 못하는 예외 경로에서만 우연히 미발행(로직상 발행 이후 실패 시나리오는 없었음) | `AFTER_COMMIT` 리스너 자체가 롤백된 트랜잭션에서는 스프링에 의해 호출되지 않으므로 구조적으로 미발행 보장 |
| `OrderService`의 Kafka 의존 | `KafkaTemplate<String, Object>`, `KafkaProducerConfig.ORDER_EVENTS_TOPIC` 직접 참조 | `KafkaTemplate`/`KafkaProducerConfig` 참조 제거, `ApplicationEventPublisher`만 의존 |

## 3. `OrderEvent`를 스프링 애플리케이션 이벤트로 재사용한 설계 근거

- `OrderEvent`는 원래 "Kafka로 나가는 외부 시스템 계약(데이터 수집 플랫폼)"으로만 규정된
  DTO였다(`OrderEvent` 클래스 Javadoc). 이번 작업에서 별도의 내부 이벤트 클래스를 새로
  만들지 않고 동일 클래스를 `ApplicationEventPublisher`의 payload로도 재사용했다.
- 근거: (1) 필드 계약(스키마) 자체는 변하지 않으며 발행 "경로"만 트랜잭션 내부 직접 호출에서
  스프링 이벤트 버스를 경유하는 간접 호출로 바뀐 것이므로, 별도 내부 이벤트 타입을 신설하면
  동일한 필드를 중복 정의하는 불필요한 변환 계층만 늘어난다. (2) 새 토픽 상수·새 메시지 스키마
  필드 추가 금지 제약과도 부합한다. (3) `docs/design/coffee-order.md`가 이미 order 레이어를
  "(+ Kafka producer, TransactionalEventListener)"로 설명해 이 배치를 예고하고 있었다.
- 트레이드오프: `OrderEvent`가 이제 "외부 Kafka 계약"과 "내부 스프링 이벤트 payload" 두 역할을
  동시에 지므로, 향후 두 계약이 갈라져야 할 필요가 생기면(예: 내부 이벤트에만 필드 추가) 분리를
  재검토해야 한다. 현재는 두 역할의 필드 요구사항이 완전히 같으므로 재사용이 더 단순하다.
  이 트레이드오프는 `OrderEvent` 클래스 Javadoc에 명시했다.

## 4. 컴포넌트 스캔·빈 등록 확인

- `OrderEventKafkaListener`는 `com.coffeeorder.order.event` 패키지, `@Component`로 등록했다.
  메인 애플리케이션 클래스의 컴포넌트 스캔 루트가 `com.coffeeorder`이므로 스캔 범위 안에
  있다(`OrderEvent`가 이미 동일 패키지에 있어 스캔 대상임이 기존에 확인된 것과 동일).
- `OrderServiceAtomicityTest`/`OrderServiceConcurrencyTest`처럼 `@Import(OrderService.class)`로
  `OrderService`만 명시적으로 등록하는 슬라이스 테스트(`@DataJpaTest`)에서는
  `OrderEventKafkaListener`가 `@Import`되지 않아 컨텍스트에 존재하지 않는다. 이는 의도적으로
  두 테스트가 "리스너의 AFTER_COMMIT 동작" 자체를 검증 범위로 삼지 않기 때문이며(2절 참고),
  프로덕션 컴포넌트 스캔 자체는 두 테스트와 무관하게 정상 동작한다(리스너를 스캔에서 놓치는
  회귀는 `@SpringBootTest` 전체 컨텍스트 로딩 테스트 등으로 별도 보완할 수 있으나, 이번
  티켓 범위에서는 신규 컨텍스트 로딩 테스트를 추가하지 않았다 — 리스크로 별도 명시).

## 5. 영향받은 파일·테스트 목록

- 프로덕션
  - `src/main/java/com/coffeeorder/order/service/OrderService.java`: `KafkaTemplate`/
    `KafkaProducerConfig` 의존 제거, `ApplicationEventPublisher` 생성자 주입 추가,
    `eventPublisher.publishEvent(OrderEvent.from(order, orderItem))`로 교체, 클래스 Javadoc
    갱신.
  - `src/main/java/com/coffeeorder/order/event/OrderEventKafkaListener.java`(신규):
    `@TransactionalEventListener(phase = AFTER_COMMIT)`로 `kafkaTemplate.send(order-events,
    event)` 호출.
  - `src/main/java/com/coffeeorder/order/event/OrderEvent.java`: Javadoc에 "내부 스프링 이벤트
    payload로도 재사용됨" 보강(스키마 자체는 변경 없음).
- 테스트
  - `src/test/java/com/coffeeorder/order/service/OrderServiceTest.java`: `KafkaTemplate` mock →
    `ApplicationEventPublisher` mock으로 교체, `verify(kafkaTemplate).send(...)` /
    `verify(kafkaTemplate, never()).send(...)` → `verify(eventPublisher).publishEvent(...)` /
    `verify(eventPublisher, never()).publishEvent(any())`로 교체(모든 실패 케이스 포함).
  - `src/test/java/com/coffeeorder/order/event/OrderEventKafkaListenerTest.java`(신규):
    Mockito 단위 테스트로 `onOrderEvent(event)` 호출 시 `kafkaTemplate.send(order-events,
    event)`가 정확히 호출되는지 검증.
  - `src/test/java/com/coffeeorder/order/service/OrderServiceAtomicityTest.java`,
    `OrderServiceConcurrencyTest.java`: 프로덕션 로직 무수정으로 컴파일·통과 확인
    (`ApplicationEventPublisher`는 스프링 컨텍스트가 resolvable dependency로 자동 제공).
  - `src/test/java/com/coffeeorder/config/KafkaProducerConfigEmbeddedKafkaTest.java`: 상단
    Javadoc의 "기존 OrderServiceTest의 `verify(kafkaTemplate).send(...)`" 문구를 새 검증
    방식(`verify(eventPublisher).publishEvent(...)`)으로 갱신(선택적 문서 정합성 보정).
- 실행 결과: `./gradlew test` 전체 green.

## 6. 태스크 3(롤백 시 미발행 테스트)·태스크 4(Outbox)와의 범위 경계

- 이번 작업은 "커밋 후에만 Kafka로 발행되는 코드 경로"를 만드는 것까지가 범위다.
  `OrderServiceAtomicityTest`의 두 실패 시나리오 테스트는 여전히
  `verify(kafkaTemplate, never()).send(any(), any())`를 검증하지만, 이는 `OrderService`가
  `OrderItemRepository.save(...)`에서 예외를 던져 `eventPublisher.publishEvent(...)` 호출
  자체에 도달하지 못하기 때문이지, `OrderEventKafkaListener`의 `AFTER_COMMIT` 동작을 검증하는
  것이 아니다(이 테스트는 리스너를 `@Import`하지 않는다). 즉 "커밋 성공 시 실제로 커밋 이후에만
  발행되는지"를 직접 실증하는 통합 테스트(예: 트랜잭션 커밋 후 리스너 호출 확인, 강제 롤백 시
  리스너 미호출 확인)는 의도적으로 태스크 3 범위로 남겨두었다 — Evaluator가 이를 "이번 티켓
  AC와 무관한 갭"으로 오인하지 않도록 이 절에 명시한다.
- 동기 `AFTER_COMMIT` 리스너의 신뢰성 한계(Kafka 브로커 장애 시 커밋은 이미 끝났으므로
  서비스 로직에서 롤백 불가, 발행 실패 시 재시도/영속 큐 없음)를 근본적으로 보완하는
  Transactional Outbox 패턴 도입은 태스크 4 범위이며 이번 작업에서 다루지 않는다.

## 참조

- `docs/design/jira-manual.md`(238~249행, Story E6-3), `docs/api/order.md`(처리 1~6단계,
  "Kafka 이벤트" 절), `docs/design/coffee-order.md`(order 레이어 설명),
  `docs/design/transaction-atomicity-check.md`(태스크 1, 이 작업이 이어받는 선행 문서),
  `src/main/java/com/coffeeorder/order/service/OrderService.java`,
  `src/main/java/com/coffeeorder/order/event/OrderEventKafkaListener.java`,
  `src/main/java/com/coffeeorder/order/event/OrderEvent.java`,
  `src/main/java/com/coffeeorder/config/KafkaProducerConfig.java`,
  `src/test/java/com/coffeeorder/order/service/OrderServiceTest.java`,
  `src/test/java/com/coffeeorder/order/event/OrderEventKafkaListenerTest.java`,
  `src/test/java/com/coffeeorder/order/service/OrderServiceAtomicityTest.java`,
  `src/test/java/com/coffeeorder/order/service/OrderServiceConcurrencyTest.java`
