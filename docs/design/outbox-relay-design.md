# Transactional Outbox 테이블 + 릴레이 설계/구현 — 데이터 일관성 (E6-3 태스크4, SCRUM-78)

- **상태**: 확정 (Plan → Generate 반영 완료. `outbox_events` 테이블·마이그레이션·엔티티·
  리포지토리·`OutboxRelay`·`SchedulingConfig`를 신규 추가하고, `OrderService`의 발행 경로를
  `ApplicationEventPublisher`/`OrderEventKafkaListener`(AFTER_COMMIT 직접 발행)에서 같은
  트랜잭션 안의 Outbox 행 저장으로 교체했다. 관련 테스트를 갱신·신규 작성했다.)
- **범위**: `jira-backlog.md`(77행)·`jira-manual.md`(249행)에 **"(확장)"**으로 명시된 태스크로,
  Story E6-3의 필수 AC("결제 롤백 시 잔액 원복 + 이벤트 미발행")는 이미 태스크1
  (`transaction-atomicity-check.md`)~태스크3(`rollback-no-publish-check.md`)에서 충족·검증이
  끝난 상태에서, 발행 자체의 **신뢰성**(브로커 장애 시 재시도·영속 큐)을 보강하는 별도 확장
  작업이다. 컨슈머 측 멱등 처리, `outbox_events` 행 보관/정리(retention) 정책, 최대 재시도
  초과 시 운영 개입은 이번 확장의 범위 밖이다(7절).

## 1. 요구사항·제약 재확인

- `docs/design/jira-backlog.md`(74~78행, Story E6-3 "③ 데이터 일관성") / `docs/design/jira-manual.md`
  (238~249행): AC는 "결제 롤백 시 잔액 원복 + 이벤트 미발행"이며, 태스크4가 "(확장)
  Transactional Outbox 테이블 + 릴레이 설계/구현"이다.
- `docs/design/rollback-no-publish-check.md`(6절)가 이 작업의 필요성을 예고: "동기
  AFTER_COMMIT 리스너는 커밋 후 `kafkaTemplate.send` 자체가 실패해도 DB 롤백을 유발하지
  못하고 재시도/영속 큐가 없다 → Transactional Outbox로 근본 보완."
- `docs/design/schema-strategy.md`: Flyway가 DDL을 소유하고 Hibernate는 `validate`만
  수행한다. 신규 테이블도 이 전략을 그대로 따른다.
- `docs/policy.md`: 저장은 UTC 기준, 감사 컬럼(`createdAt`/`updatedAt`)은 JPA Auditing으로
  자동 관리.
- `CLAUDE.md`: "계획에 없는 범위 확장(불필요한 리팩터링 등) 금지." 이번 작업은 "Kafka 발행
  책임을 `OrderService`/`OrderEventKafkaListener`에서 Outbox 테이블 + 릴레이로 이전"하는
  것으로 수정 범위를 한정했다.

## 2. 설계 결정과 근거

### 2-1. 스키마 (`outbox_events`)

`docs/db/schema.md` § outbox_events, `src/main/resources/db/migration/V3__create_outbox_events.sql`.

| 컬럼 | 타입 | 근거 |
| --- | --- | --- |
| `aggregate_type`/`aggregate_id` | VARCHAR(50)/BIGINT | 특정 이벤트 타입(`OrderEvent`)에 결합하지 않은 범용 필드. 현재는 `"ORDER"`/`orders.id`만 쓰지만, 향후 다른 애그리게잇의 이벤트도 같은 테이블·릴레이를 재사용할 수 있게 설계했다. |
| `topic` | VARCHAR(100) | 발행될 Kafka 토픽명(`order-events`). 릴레이가 이 값으로 `kafkaTemplate.send(topic, payload)`를 호출하므로 토픽을 하드코딩하지 않는다. |
| `payload` | VARCHAR(4000) | 발행할 메시지 본문의 JSON 직렬화 문자열. 최초에는 `TEXT`로 설계했으나(계획 문서), H2(MySQL 호환 모드)에서 `TEXT` 컬럼이 `CHARACTER VARYING`으로 생성되어 Hibernate `@Lob`(CLOB 기대)과 `ddl-auto: validate`가 충돌함을 로컬 테스트로 확인했다(`SchemaManagementException: found [character varying], but expecting [clob]`). `OrderEvent` payload 실측 크기(약 150바이트 내외)를 고려해 `TEXT`/`@Lob` 대신 다른 VARCHAR 컬럼과 동일한 방식(길이 명시)의 `VARCHAR(4000)`로 스키마·마이그레이션·엔티티를 함께 수정해 MySQL/H2 양쪽에서 `validate`가 안전하게 통과하도록 했다(`docs/db/schema.md`도 이 결정을 반영해 갱신). |
| `status` | VARCHAR(20), DEFAULT `PENDING` | `orders.status`와 동일하게 `CHECK` 제약 없는 자유 문자열 컬럼으로 통일(기존 컨벤션 유지). |
| `retry_count` | INT, DEFAULT 0 | 발행 재시도 횟수. |
| `created_at`/`updated_at`/`sent_at` | DATETIME(6) | 감사 컬럼(자동 관리) + 발행 성공 시각(`sent_at`, 성공 전 NULL). |
| `idx_outbox_events_status_created_at` | INDEX(status, created_at) | 릴레이 폴링 쿼리(`WHERE status = 'PENDING' ORDER BY created_at ASC`)를 인덱스로 지원. |

### 2-2. 발행 책임 이전 — `OrderService` / `OrderEventKafkaListener`

- **이전(SCRUM-76/77)**: `OrderService.order()`가 트랜잭션 내부에서
  `ApplicationEventPublisher.publishEvent(OrderEvent)`를 호출하고, `OrderEventKafkaListener`
  (`@TransactionalEventListener(phase = AFTER_COMMIT)`)가 커밋 후 직접
  `kafkaTemplate.send(...)`를 호출했다.
- **이후(SCRUM-78)**: `OrderService.order()`가 포인트 차감·주문/주문항목 저장과 **같은
  물리 트랜잭션 안에서** `OutboxEventRepository.save(new OutboxEvent("ORDER", order.getId(),
  KafkaProducerConfig.ORDER_EVENTS_TOPIC, payload))`를 호출한다. `payload`는
  `OrderEvent.from(order, orderItem)`을 `KafkaProducerConfig.orderEventObjectMapper()`(기존
  `producerFactory()`가 쓰던 것과 동일한 Jackson 설정, `WRITE_DATES_AS_TIMESTAMPS=false`)로
  JSON 문자열 직렬화한 값이다. 저장이 실패(롤백)하면 Outbox 행도 함께 롤백되므로, 태스크1이
  이미 증명한 트랜잭션 원자성을 Outbox 삽입도 그대로 물려받는다(추가 보상 로직 불필요).
- 실제 Kafka 발행 책임은 `OrderService`/트랜잭션에서 완전히 분리해 `OutboxRelay`(트랜잭션
  밖에서 폴링)로 옮겼다. 이에 따라 `OrderEventKafkaListener`(`AFTER_COMMIT` 직접 발행 리스너,
  SCRUM-76 산출물)는 더 이상 어떤 이벤트도 수신하지 않는 죽은 코드가 되어 삭제했다(관련
  단위 테스트 `OrderEventKafkaListenerTest`도 함께 삭제). `OrderService`는 더 이상
  `ApplicationEventPublisher`/`KafkaTemplate` 어느 쪽에도 의존하지 않는다(생성자에서
  `ApplicationEventPublisher` 파라미터를 `OutboxEventRepository`로 교체).
- `KafkaProducerConfig.orderEventObjectMapper()`를 `producerFactory()`에서 쓰던 인라인
  구현에서 `public static` 메서드로 추출해, `OrderService`(payload 직렬화)와
  `OutboxRelay`(payload 역직렬화)가 동일한 직렬화 규약을 공유하도록 했다(단일 소스 유지 —
  세 곳에서 각자 `ObjectMapper`를 다르게 구성해 규약이 갈라지는 것을 방지). 두 클래스 모두
  이 메서드를 생성자에서 호출해 `private final ObjectMapper` 필드로 보관할 뿐, 별도
  스프링 빈으로 등록하지는 않았다(순수 유틸리티 구성이라 DI 계약을 늘릴 필요가 없고, 기존
  `OrderService` 관련 테스트들의 `@Import` 목록을 건드리지 않아도 되는 이점도 있다).

### 2-3. `OutboxRelay`의 트랜잭션 경계

- `OutboxRelay`에는 자체 `@Transactional` 메서드를 두지 않았다. `OutboxEventRepository`
  (Spring Data JPA `SimpleJpaRepository`)의 `save`/`findByStatusOrderByCreatedAtAsc`는 그
  자체로 각각 독립된 트랜잭션에서 실행되므로, 한 행의 Kafka 발행 성공/실패가 다른 행의 처리에
  영향을 주지 않는다.
- 만약 `relayPendingEvents()`(락 콜백을 거치는 진입점) 안에서 `this.xxx()` 형태로 같은
  객체의 `@Transactional` 메서드를 호출했다면 self-invocation으로 그 애너테이션이 무시되는
  함정이 있었을 것이다(`docs/design/transaction-atomicity-check.md` 2절 (e)항과 동일한
  패턴). 리포지토리 메서드 단위로 트랜잭션 경계를 좁게 유지해 이 함정을 피했다.

### 2-4. 다중 인스턴스 중복 발행 방지 — 기존 `RedisDistributedLock` 재사용

- 새 락 유틸을 만들지 않고 `com.coffeeorder.common.lock.RedisDistributedLock`(E6-2 산출물)을
  고정 키 `"outbox-relay"`로 재사용했다. `waitTime`을 짧게(200ms) 잡아 "락을 얻지 못하면 오래
  기다리지 않고 이번 폴링을 건너뛴다"는 정책을 구현했고, `leaseTime`(10초)은 한 배치 처리
  시간을 여유 있게 덮도록 잡았다.
- 락 획득 실패는 `BusinessException(ErrorCode.CONCURRENCY_CONFLICT)`로 알려지므로, 이를
  "다른 인스턴스가 처리 중"으로 간주해 조용히 스킵한다(다른 `ErrorCode`의
  `BusinessException`은 예상치 못한 오류이므로 그대로 전파).

### 2-5. 스케줄링 인프라 — `SchedulingConfig`

- 이 프로젝트에 처음 도입되는 `@Scheduled`/`@EnableScheduling`이다. 메인 애플리케이션
  클래스가 아닌 별도 `config.SchedulingConfig`(`JpaAuditingConfig` 선례)로 분리했다.
- `test` 프로파일에는 실제 Kafka 브로커가 없어 자동 폴링을 그대로 두면 불필요한 연결 시도·
  타이밍 경합이 생긴다. `KafkaProducerConfig`의 `KafkaAdmin`/`NewTopic` 빈을 `test`
  프로파일에서 비활성화하는 것과 동일한 패턴으로, `SchedulingConfig` 클래스 자체를
  `@Profile("!test")`로 제한해 `ScheduledAnnotationBeanPostProcessor`가 `test` 컨텍스트에
  아예 등록되지 않게 했다. `OutboxRelay` 빈 자체(와 `@Scheduled` 애너테이션)는 프로파일 제한
  없이 항상 등록되므로, 테스트는 `relayPendingEvents()`를 직접 호출해 결정론적으로 검증한다.

## 3. 폴링·재시도·멱등성 정책

- **폴링**: `OutboxRelay.relayPendingEvents()`가 `@Scheduled(fixedDelayString =
  "${outbox.relay.fixed-delay-ms:1000}")`(기본 1초 간격, 별도 프로퍼티 파일 변경 없이 기본값만
  사용)로 트리거되며, `outboxEventRepository.findByStatusOrderByCreatedAtAsc(PENDING,
  PageRequest.of(0, 50))`로 오래된 순 최대 50건을 배치 조회한다(`idx_outbox_events_status_created_at`
  인덱스 활용).
- **발행 시도**: 각 행마다 `payload`(JSON 문자열)를 `Map<String,Object>`로 역직렬화해
  `kafkaTemplate.send(topic, map)`를 호출하고 `get(5, TimeUnit.SECONDS)`로 동기 대기한다.
  `Map`으로 역직렬화 후 재발행하는 이유: `KafkaTemplate`의 `JsonSerializer`가 이미
  `orderEventObjectMapper()`와 동일한 설정으로 값을 직렬화하므로, 저장된 JSON 문자열의 필드
  구조(`orderId`/`userId`/`menuId`/`amount`/`orderedAt` 등 키·값)가 그대로 보존된 채 다시
  JSON으로 나간다(키 순서는 원본과 다를 수 있으나 JSON 의미상 동일).
- **성공**: `OutboxEvent.markSent(now)`로 `status=SENT`, `sentAt` 기록 후 저장. 다음 폴링부터
  이 행은 `status=PENDING` 조건에서 제외되어 다시 조회되지 않는다.
- **실패(어떤 예외든)**: `OutboxEvent.recordFailure()`로 `retryCount`만 증가시키고 상태는
  `PENDING`을 유지한 채 저장한다. 예외를 `relayOne` 밖으로 전파하지 않으므로, 배치 안의 한
  행이 실패해도 나머지 행 처리가 계속된다(다음 폴링 사이클에서 실패한 행도 다시 조회되어
  재시도된다 — **at-least-once**).
- **멱등성/최대 재시도**: `OutboxEventStatus`에 `FAILED` 값을 스키마·enum 수준에서 예약해
  뒀지만, 이번 구현은 최대 재시도 횟수 초과 시 `FAILED`로 자동 전이시키는 정책을 두지 않는다
  (계획에 명시되지 않은 범위이므로 임의로 정책을 추가하지 않았다 — 7절 "범위 밖" 참고).
  컨슈머 측에서 같은 이벤트를 중복 수신할 수 있음(at-least-once)을 전제로, 멱등 처리는 이
  프로젝트의 컨슈머(별도 "데이터 수집 플랫폼", 이 저장소 범위 밖)가 책임진다.

## 4. 테스트 내역

| 파일 | 목적 |
| --- | --- |
| `src/test/java/com/coffeeorder/order/outbox/repository/OutboxEventRepositoryTest.java`(신규) | `OutboxEvent` 저장/조회, `findByStatusOrderByCreatedAtAsc`가 `PENDING`만 오래된 순으로 배치 반환, `markSent`/`recordFailure` 상태 전이를 `@DataJpaTest`(H2)로 검증(Step2 성공 기준). |
| `src/test/java/com/coffeeorder/order/outbox/OutboxRelayTest.java`(신규) | `OutboxRelay`의 Mockito 단위 테스트. (a) 발행 성공 시 `kafkaTemplate.send` 1회 + 행 `SENT` 전이, (b) 발행 실패 시 예외 미전파 + `PENDING` 유지·`retryCount` 증가, (c) `PENDING` 행이 없으면 미호출, (d) 락 획득 실패 시 조용히 스킵(Step4 성공 기준 a·b·c 모두 포함). |
| `src/test/java/com/coffeeorder/order/service/OrderServiceOutboxTest.java`(신규) | 정상 주문이 커밋되면 `outbox_events`에 `PENDING` 행 1건이 생성되고 `payload`가 `OrderEvent` 스키마(필드명·값·`orderedAt` ISO 문자열)와 일치함을 통합 테스트로 검증(Step3 성공 기준 1번). |
| `src/test/java/com/coffeeorder/order/service/OrderServiceAtomicityTest.java`(갱신) | 기존 두 강제 실패 시나리오(기존 사용자 UPDATE 롤백, 신규 사용자 INSERT 롤백)에 `outbox_events` 행이 전혀 남지 않는지 확인하는 어서션을 추가하고, 더 이상 필요 없는 `KafkaTemplate` mock 빈(`KafkaTemplateTestConfig`)과 `verify(kafkaTemplate, never())`를 제거했다(Step3 성공 기준 2·3번). |
| `src/test/java/com/coffeeorder/order/service/OrderServiceConcurrencyTest.java`(갱신) | `OrderService`가 더 이상 `KafkaTemplate`에 의존하지 않으므로 불필요해진 `KafkaTemplateTestConfig`를 제거하고, 동시 주문으로 생성된 `outbox_events` 행도 `@AfterEach`에서 함께 정리하도록 보강(회귀 확인용, 로직 자체는 무수정). |
| `src/test/java/com/coffeeorder/order/service/OrderServiceTest.java`(갱신) | Mockito 단위 테스트. `ApplicationEventPublisher` mock을 `OutboxEventRepository` mock으로 교체하고, `verify(eventPublisher).publishEvent(...)` 대신 저장된 `OutboxEvent`의 `aggregateType`/`topic`/`status`/`payload`(JSON 역직렬화 후 필드 단위 비교)를 검증하도록 전면 교체. |

### 4-1. 삭제된 파일과 사유

- `src/main/java/com/coffeeorder/order/event/OrderEventKafkaListener.java`: `OrderService`가
  더 이상 `ApplicationEventPublisher.publishEvent(OrderEvent)`를 호출하지 않으므로 이
  `@TransactionalEventListener(AFTER_COMMIT)` 리스너는 어떤 이벤트도 수신하지 못하는 죽은
  코드가 된다. 발행 책임이 `OutboxRelay`로 완전히 이전됐으므로 삭제했다.
- `src/test/java/com/coffeeorder/order/event/OrderEventKafkaListenerTest.java`: 위 리스너의
  단위 테스트로, 대상 프로덕션 클래스 삭제와 함께 제거했다.
- `src/test/java/com/coffeeorder/order/service/OrderServiceAfterCommitPublishTest.java`
  (SCRUM-77 산출물): "커밋되면 `OrderEventKafkaListener`가 실제로 호출되어 Kafka로 1회
  발행됨"/"롤백되면 호출되지 않음"을 검증하던 통합 테스트. 두 시나리오 모두 전제(리스너를
  통한 AFTER_COMMIT 직접 발행)가 이번 설계 변경으로 사라졌으므로, "커밋 시 발행 트리거 생성"은
  `OrderServiceOutboxTest`(신규)가, "롤백 시 미발행(=Outbox 행도 생성되지 않음)"은
  `OrderServiceAtomicityTest`(갱신)가 각각 대체해 이어받는다.

## 5. 회귀 확인

- `./gradlew test` 전체 실행 결과: 115개 테스트 전부 green(`failures=0, errors=0`).
- 특히 `OrderServiceAtomicityTest`, `OrderServiceConcurrencyTest`, `PointServiceConcurrencyTest`가
  이번 변경(발행 경로 교체 + 관련 테스트 갱신)으로 깨지지 않음을 확인했다 — 포인트 차감/락/
  비관적 락 로직 자체는 무수정이므로 동시성 테스트의 락 순서·타이밍에는 영향이 없다.
- Flyway 마이그레이션(`V3__create_outbox_events.sql`)이 H2(MySQL 호환 모드)에서 오류 없이
  적용되고, Hibernate `ddl-auto: validate`가 `OutboxEvent` 엔티티 매핑과 정확히 일치함을
  `@SpringBootTest`(`CoffeeorderApplicationTests`) 컨텍스트 로딩으로 확인했다(2-1절에 기록한
  `payload` 타입 재조정 이후 통과).

## 6. 설계 시행착오 — `payload` 컬럼 타입

초기 계획(Step1)은 `payload TEXT`(+ 엔티티 `@Lob`)였으나, 실제 `./gradlew test` 실행 시
`OutboxEvent` 매핑에서 `SchemaManagementException: Schema validation: wrong column type
encountered in column [payload] in table [outbox_events]; found [character varying
(Types#VARCHAR)], but expecting [clob (Types#CLOB)]`가 발생했다. H2(MySQL 호환 모드)가
`TEXT` DDL 타입을 `CHARACTER VARYING`(VARCHAR)으로 생성하기 때문에, `@Lob`(CLOB 기대)과
`ddl-auto: validate`가 충돌한 것이다. `OrderEvent` payload의 실제 크기(수백 바이트 수준)를
감안해 CLOB/TEXT 대신 다른 VARCHAR 컬럼과 동일한 방식으로 `VARCHAR(4000)`(엔티티는
`@Column(length = 4000)`, `@Lob` 제거)로 스키마·마이그레이션·엔티티 세 곳을 함께 수정해
해결했다. `docs/db/schema.md`/`V3__create_outbox_events.sql`/`OutboxEvent.java`가 이 최종
결정을 반영한다(원래 계획 문서의 `TEXT` 표기와 다르지만, 이는 Generate 단계에서 실제 테스트로
발견해 수정한 구현 세부사항이다).

## 7. 후속 범위 경계 (이번 확장이 다루지 않는 것)

- **컨슈머 측 멱등 처리**: at-least-once 전달만 보장하므로, 동일 이벤트가 중복 발행될 수
  있다(예: `kafkaTemplate.send`는 성공했지만 그 직후 `markSent` 저장 전에 프로세스가 죽는
  경우). 컨슈머(데이터 수집 플랫폼, 이 저장소 범위 밖)가 `orderId` 등을 기준으로 멱등하게
  처리해야 한다.
- **`outbox_events` 행 보관/정리(retention) 정책**: `SENT` 행을 언제까지 보관할지, 별도
  배치로 삭제/아카이빙할지는 정의하지 않았다. 현재는 무기한 누적된다.
- **최대 재시도 초과 시 처리**: `retryCount`만 계속 증가할 뿐, 일정 횟수를 넘기면 `FAILED`로
  전이해 운영자가 개입하게 하는 정책은 스펙에 없어 구현하지 않았다(`OutboxEventStatus.FAILED`
  값은 스키마·enum 수준에서만 예약).
- **정확히-한번(exactly-once) 보장**: Redis 락으로 동시 폴링 인스턴스 수를 1개로 좁히지만,
  락 TTL 만료·프로세스 크래시 등 경계 상황까지 완전히 막지는 못한다(at-least-once).
- **`OutboxRelay` 자체의 수평 확장/샤딩**: 현재는 "락을 가진 인스턴스 1개가 전체 배치를
  처리"하는 단순 구조다. 처리량이 커졌을 때의 파티셔닝/샤딩 전략은 범위 밖이다.
- **`docs/api/order.md`/`docs/design/coffee-order.md` 갱신**: 두 문서는 "커밋 후 Kafka
  발행"이라는 관찰 가능한 계약 자체는 여전히 유효하므로(Outbox+릴레이도 결과적으로 커밋 이후
  비동기 발행), 이번 확장의 명시적 수정 대상 파일 목록에 포함하지 않았다(범위 확장 금지
  원칙). 두 문서가 언급하는 "TransactionalEventListener"라는 구현 세부사항이 이제
  "Transactional Outbox + 폴링 릴레이"로 바뀐 점은 이 문서와 `OrderService`/`OrderEvent`
  Javadoc이 최신 근거를 제공한다.

## 참조

- `docs/design/jira-backlog.md`(74~78행, Story E6-3), `docs/design/jira-manual.md`
  (238~249행), `docs/design/kafka-after-commit-check.md`(태스크2),
  `docs/design/rollback-no-publish-check.md`(태스크3, 특히 6절 "태스크4와의 범위 경계"),
  `docs/design/transaction-atomicity-check.md`(태스크1, self-invocation 함정 재참조),
  `docs/design/schema-strategy.md`, `docs/db/schema.md`, `docs/policy.md`,
  `docs/code-convention.md`,
  `src/main/resources/db/migration/V1__init_schema.sql`(마이그레이션 스타일 선례),
  `src/main/resources/db/migration/V3__create_outbox_events.sql`(신규),
  `src/main/java/com/coffeeorder/order/outbox/entity/OutboxEvent.java`(신규),
  `src/main/java/com/coffeeorder/order/outbox/entity/OutboxEventStatus.java`(신규),
  `src/main/java/com/coffeeorder/order/outbox/repository/OutboxEventRepository.java`(신규),
  `src/main/java/com/coffeeorder/order/outbox/OutboxRelay.java`(신규),
  `src/main/java/com/coffeeorder/config/SchedulingConfig.java`(신규),
  `src/main/java/com/coffeeorder/config/KafkaProducerConfig.java`(`orderEventObjectMapper()`
  추출),
  `src/main/java/com/coffeeorder/order/service/OrderService.java`(발행 경로 교체),
  `src/main/java/com/coffeeorder/order/event/OrderEvent.java`(Javadoc 갱신),
  `src/main/java/com/coffeeorder/common/lock/RedisDistributedLock.java`(재사용),
  `src/test/java/com/coffeeorder/order/outbox/repository/OutboxEventRepositoryTest.java`(신규),
  `src/test/java/com/coffeeorder/order/outbox/OutboxRelayTest.java`(신규),
  `src/test/java/com/coffeeorder/order/service/OrderServiceOutboxTest.java`(신규),
  `src/test/java/com/coffeeorder/order/service/OrderServiceAtomicityTest.java`(갱신),
  `src/test/java/com/coffeeorder/order/service/OrderServiceConcurrencyTest.java`(갱신),
  `src/test/java/com/coffeeorder/order/service/OrderServiceTest.java`(갱신)
