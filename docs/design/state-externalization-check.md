# 상태 외부화 확인 (잔액=MySQL, 락=Redis, 스트림=Kafka) — 다수 인스턴스 무상태 설계 (E6-1 태스크 2)

- **상태**: 확정 (Plan → Generate 반영 완료. 코드/설정을 점검한 결과만 문서화하며,
  이번 작업에서 코드 변경은 발생하지 않음)
- **범위**: "상태를 들고 있어야 하는 부분"(잔액, 동시성 제어를 위한 락, 이벤트 스트림)이
  인스턴스 로컬이 아닌 외부 저장소/브로커에 실제로 위치하는지 코드·설정 근거로 확인한다.
  `HttpSession`/로컬 캐시 등 "인스턴스가 상태를 로컬로 들고 있지 않은지"는 이미
  `docs/design/session-cache-check.md`에서 확인했으므로 이 문서에서는 재조사하지 않고
  인용만 한다(아래 6절). Redis 의존성 추가·분산락 유틸 구현·비관적 락 적용 등 신규 코드
  작성은 이 문서의 범위 밖이다(별도 스토리 E6-2, 아래 6절 참고).

## 1. 요구사항·제약 재확인

- `CLAUDE.md`: "기능 작업은 계획 → 구현 → 독립 검증 순서로 진행합니다. 계획에 없는 범위
  확장(불필요한 리팩터링 등) 금지."
- `docs/design/jira-manual.md`(212~223행, Story E6-1 "① 다수 인스턴스 무상태 설계"):
  수용 기준은 "인스턴스 증설이 기능에 영향 없음을 근거로 설명·시연"이며, 태스크 2가
  "상태 외부화 확인 (잔액=MySQL, 락=Redis, 스트림=Kafka)"이다. 태스크 1(세션/로컬 캐시
  점검·제거)과 태스크 3(다중 인스턴스 기동 시나리오 문서화)은 별개 태스크로, 이 문서는
  태스크 2만 다룬다.
- `docs/design/jira-backlog.md`(E6-①): 동일 스토리/태스크 정의("Task: 세션/로컬 캐시
  제거, 상태 외부화(MySQL/Redis) 확인").
- `docs/design/jira-backlog.md`(E6-②) / `docs/design/jira-manual.md`(224~236행,
  Story E6-2 "② 동시성 제어"): "Redis 의존성 추가 + 분산락 유틸", "PointBalance 비관적
  락 조회(`@Lock PESSIMISTIC_WRITE`)", "충전·차감·주문 경로에 비관적 락 + 분산락 적용"이
  **별도 스토리(E6-2)의 태스크**로 명시되어 있다. 즉 락을 "실제로 Redis로 구현"하는 작업은
  SCRUM-20(E6-1)이 아니라 E6-2의 책임이며, 이 문서(E6-1 태스크 2)는 그 구현 여부를
  "확인"하는 것이지 구현 자체가 아니다.
- `docs/design/session-cache-check.md`(5절): "상태 외부화 확인(E6-1 태스크 2) … Redis
  등 신규 외부 저장소 도입/검증은 다루지 않는다(이번 작업에서 Redis 관련 코드를 추가하지
  않음)"이라고 이미 예고되어 있다.
- 전제: 잔액·락·이벤트 스트림처럼 여러 인스턴스가 동시에 접근·공유해야 하는 상태가
  인스턴스 프로세스 내부(힙 메모리)에만 있으면, 로드밸런서 뒤 인스턴스가 여러 개일 때
  인스턴스마다 값이 달라지거나 유실될 수 있다. 이를 코드/설정 근거로 확인하는 것이 이번
  태스크의 목적이며, 확인 결과 아직 외부화되지 않은 항목이 있다면 "왜 그런지"와 "어느
  스토리에서 다루는지"를 함께 밝혀 상위 AC와 모순되지 않게 한다(4절 결론 참고).

## 2. 점검 방법

대상 경로: `src/main/java/com/coffeeorder/point`, `src/main/java/com/coffeeorder/order`,
`src/main/java/com/coffeeorder/config`, `build.gradle`, `docker-compose.yml`,
`docs/db/schema.md`.

검색/확인 명령(재현 가능):

```bash
# 2-1. 잔액 — MySQL(JPA) 외 다른 저장 경로가 있는지
grep -rn "balance" src/main/java/com/coffeeorder/point --include="*.java" -l
grep -n "point_balances" -A 10 docs/db/schema.md

# 2-2. 스트림 — Kafka 발행 경로, 로컬 큐/리스트 사용 여부
grep -n "kafkaTemplate.send" src/main/java/com/coffeeorder/order/service/OrderService.java
grep -n "ORDER_EVENTS_TOPIC" src/main/java/com/coffeeorder/config/KafkaProducerConfig.java
grep -n "kafka:" docker-compose.yml

# 2-3. 락 — Redis/분산락/비관적 락 도입 여부
grep -rn "Redis\|PESSIMISTIC\|@Lock" src/main/java build.gradle
grep -n "redis\|Redis" build.gradle
```

## 3. 점검 결과 — 잔액(MySQL)

- `point/entity/PointBalance.java`: `@Entity @Table(name = "point_balances")`로 JPA
  매핑된 엔티티. 필드는 `userId`(PK), `balance`, `version`(낙관적 락용),
  `updatedAt`뿐이며, 인스턴스가 별도로 들고 있는 캐시 필드는 없다.
- `point/repository/PointBalanceRepository.java`: `JpaRepository<PointBalance, Long>`를
  상속하는 스프링 데이터 리포지토리로, 잔액 조회/저장은 이 리포지토리(→ MySQL
  `point_balances` 테이블)를 경유하는 것이 유일한 경로다. 커스텀 인메모리 저장 로직은
  없다(주석: "비관적 락(`@Lock(PESSIMISTIC_WRITE)`)은 도전 과제(E6) 범위이므로 이번
  티켓에서는 추가하지 않는다").
- `point/service/PointService.java#charge()`: `pointBalanceRepository.findById(userId)`로
  조회 → `pointBalance.charge(amount)` → `pointBalanceRepository.save(pointBalance)`로
  저장. 조회·저장 모두 리포지토리(MySQL)를 거치며 서비스 인스턴스 필드에 잔액을 보관하지
  않는다.
- `order/service/OrderService.java#order()`(82~89행): 주문 시에도 동일하게
  `pointBalanceRepository.findById(userId)`로 조회 → `pointBalance.deduct(totalAmount)` →
  `pointBalanceRepository.save(pointBalance)`로 저장. 잔액 부족 판정도 방금 조회한
  `PointBalance` 엔티티 값을 그대로 비교한다.
- `docs/db/schema.md`(17~27행) `point_balances` 테이블 정의: `user_id`(PK, FK→users.id),
  `balance`(DEFAULT 0, CHECK >= 0), `version`(낙관적 락 병행용), `updated_at`. 잔액을
  `users`와 분리한 이유가 "충전/차감 시 행 단위 비관적 락을 잔액 행에만 걸어 잠금 범위를
  최소화하기 위함"이라고 명시되어 있어, 잔액 저장소가 처음부터 MySQL 테이블로 설계됐음을
  뒷받침한다.
- 인스턴스 로컬 캐시·정적 필드로 잔액을 들고 있지 않는다는 사실은
  `docs/design/session-cache-check.md`(3-2절)에서 이미 확인된 "인스턴스 필드 중 요청 간
  공유되는 Map/List/Set/카운터가 없다"는 결론과도 일치한다(중복 조사 없이 인용).

**결론**: 잔액은 오직 MySQL `point_balances` 테이블(JPA `PointBalanceRepository` 경유)에만
존재하며, 여러 인스턴스가 동시에 이 테이블을 공유해도 인스턴스 로컬 값 불일치는 발생하지
않는다(다만 동시 갱신 시 분실 갱신 방지는 락의 몫이며 4절에서 별도로 다룬다).

## 4. 점검 결과 — 스트림(Kafka)

- `config/KafkaProducerConfig.java`: `ORDER_EVENTS_TOPIC = "order-events"` 상수와 함께
  `producerFactory()`/`kafkaTemplate(ProducerFactory<String, Object>)` 빈을 등록한다.
  `bootstrapServers`는 `application-dev.yml`의 `spring.kafka.bootstrap-servers`
  (기본값 `localhost:9092`)를 `@Value`로 주입받아 사용하며, 인스턴스 내부 큐 구현이 아니다.
- `order/service/OrderService.java`(99행): 주문 결제 성공 후
  `kafkaTemplate.send(KafkaProducerConfig.ORDER_EVENTS_TOPIC, OrderEvent.from(order,
  orderItem));`로 Kafka 브로커의 `order-events` 토픽에 발행한다. `OrderService` 필드 중
  이벤트를 임시로 쌓아두는 `List`/`Queue` 등의 인스턴스 상태는 없다(생성자 주입된
  `KafkaTemplate<String, Object> kafkaTemplate` 필드만 존재).
- `order/event/OrderEvent.java`: Kafka로 나가는 메시지 스키마(`orderId`, `userId`,
  `menuId`, `amount`, `orderedAt`)만 정의하는 불변 값 객체이며, 상태를 보관하는 저장소가
  아니다.
- `docker-compose.yml`(27~50행): `kafka` 서비스가 별도 컨테이너(`apache/kafka:3.7.0`,
  KRaft 단일 노드)로 정의되어 있고, 앱은 `localhost:9092`로 접속한다. 즉 이벤트 스트림은
  애플리케이션 프로세스 밖의 별도 브로커가 담당한다.
- `src/test/java/com/coffeeorder/config/KafkaProducerConfigEmbeddedKafkaTest.java`
  (53행 `@EmbeddedKafka(partitions = 1, topics = KafkaProducerConfig.ORDER_EVENTS_TOPIC)`,
  93행 `kafkaTemplate.send(...)`): 발행된 메시지를 실제 Kafka 프로토콜(인메모리 브로커)
  위에서 소비해 토픽/스키마를 검증하는 테스트로, 발행 경로가 인메모리 리스트가 아니라
  Kafka 클라이언트 프로토콜을 통해 나간다는 점을 뒷받침한다.

**결론**: 주문 이벤트 스트림은 인스턴스 내부 상태(큐/리스트)가 아니라 별도 Kafka
브로커(`order-events` 토픽)로 발행되며, 여러 인스턴스가 각자 발행해도 이벤트가 유실되거나
인스턴스별로 분리 저장되지 않는다.

## 5. 점검 결과 — 락(Redis) 현황

- `build.gradle`(21~48행): 의존성 목록에 Redis 관련 항목(`spring-boot-starter-data-redis`,
  `lettuce`, `redisson` 등)이 없다. `grep -n "redis\|Redis" build.gradle`은 매치 없음
  (exit code 1).
- `grep -rn "Redis\|PESSIMISTIC\|@Lock" src/main/java build.gradle` 실행 결과, 코드로
  존재하는 매치는 없고 아래 **주석**(설계 의도를 명시한 문서화 목적) 3건만 매치됐다:
  - `point/service/PointService.java`(18행): "비관적 락/Redis 분산락 등 동시성 제어는
    이 서비스의 범위가 아니다(E6 도전 과제)."
  - `point/repository/PointBalanceRepository.java`(13행): "비관적 락(`@Lock
    (PESSIMISTIC_WRITE)`)은 도전 과제(E6) 범위이므로 이번 티켓(SCRUM-55)에서는 추가하지
    않는다."
  - `order/service/OrderService.java`(34행): "3단계(잔액 조회 시 비관적 락)·다중 인스턴스
    동시 주문을 막는 Redis 분산락·발행-트랜잭션 정합성 강화(AFTER_COMMIT/Outbox)는 도전
    과제(E6) 범위이며 이번 티켓에서는 다루지 않는다."
  - 즉 `RedisConfig`, 분산락 유틸 클래스, `@Lock(PESSIMISTIC_WRITE)` 애너테이션이 붙은
    리포지토리 메서드 등 **실제 코드는 존재하지 않으며**, 세 파일 모두 "아직 도입하지
    않았다"는 사실을 의도적으로 주석에 남겨 두고 있다.
- `docs/db/schema.md`(17~27행)의 `point_balances` 절도 "충전/차감 시 행 단위 비관적
  락(`SELECT ... FOR UPDATE`)을 잔액 행에만 걸어 잠금 범위를 최소화하기 위함"이라고 락의
  **설계 의도**만 기술할 뿐, 현재 적용 여부에 대해서는 `version`(낙관적 락 병행용) 컬럼만
  실제로 존재한다.
- 이 결과가 계획 범위 밖(Redis 신규 구현)이 아니라 **이미 계획된 순서**임은
  `docs/design/jira-backlog.md`(E6-②)와 `docs/design/jira-manual.md`(224~236행, Story
  E6-2 "② 동시성 제어")에 별도 스토리로 명확히 문서화되어 있다: "Redis 의존성 추가 +
  RedisConfig 작성", "분산락 유틸 작성(`point:{userId}`, 타임아웃/해제 보장)",
  "PointBalance 비관적 락 조회(`@Lock PESSIMISTIC_WRITE`)", "충전·차감·주문 경로에
  비관적 락 + 분산락 적용", "동시성 테스트"가 모두 E6-2의 태스크로 나열되어 있다.

**결론**: 현재 시점에는 락을 Redis로 외부화한 코드가 **존재하지 않는다**(확인 불가가
아니라 "미도입"이 사실로 확인됨). 이는 결함이 아니라 백로그 상 계획된 순서로, 락 도입은
별도 스토리 E6-2("② 동시성 제어")의 책임이며 SCRUM-20이 속한 E6-1("① 다수 인스턴스
무상태 설계")의 범위 밖이다.

## 6. 결론

- **잔액**: MySQL `point_balances` 테이블(JPA `PointBalanceRepository` 경유)에만
  존재 — 외부화 완료(3절).
- **스트림**: Kafka `order-events` 토픽(별도 브로커 컨테이너)으로 발행 — 외부화
  완료(4절).
- **락**: 현재 코드에 Redis 분산락/비관적 락 적용이 없음 — 아직 외부화되지 않았으며,
  이는 별도 스토리 E6-2에서 다룰 계획된 순서다(5절).
- 이 결과는 상위 AC("인스턴스 증설이 기능에 영향 없음을 근거로 설명·시연")와 모순되지
  않는다. AC가 요구하는 "무상태 설계"는 **인스턴스가 로컬 상태를 들고 있지 않아 어느
  인스턴스로 요청이 가도 동일하게 동작한다**는 관점이며, 이번 점검(3·4절)과
  `session-cache-check.md`가 이를 확인했다. 반면 "동시에 여러 요청이 같은 잔액을
  갱신할 때 분실 갱신 없이 정합성을 지키는가"는 **동시성 제어** 관점으로, 인스턴스 개수와
  무관하게(단일 인스턴스에서도) 필요한 별도 요구사항이며 E6-2의 책임이다. 즉 "무상태
  설계"와 "락을 통한 동시성 정합성"은 서로 다른 축의 요구사항이고, 이번 문서는 전자만
  다룬다.

## 7. 후속 태스크와의 범위 경계

- **세션/로컬 캐시 점검**(E6-1 태스크 1): `docs/design/session-cache-check.md`에서 이미
  완료. 이 문서는 그 결과(로컬 캐시 없음)를 3절에서 인용만 하고 재조사하지 않았다.
- **다중 인스턴스 기동 시나리오 문서화**(E6-1 태스크 3): `docker-compose scale` 등으로
  실제 다중 인스턴스를 띄워 시연하는 태스크. 이 문서는 코드/설정 레벨의 정적 점검까지만
  다루고, 실제 다중 인스턴스 기동·시연은 다루지 않는다.
- **동시성 제어**(Story E6-2): Redis 의존성 추가, `RedisConfig`, 분산락 유틸,
  `PointBalance` 비관적 락 조회, 충전·차감·주문 경로에 락 적용, 동시성 테스트는 모두
  이 문서의 범위 밖이며 별도 스토리(E6-2)에서 코드로 구현한다. 이 문서는 "락이 아직
  도입되지 않았다"는 사실을 확인·기록할 뿐, Redis 관련 코드를 추가하지 않는다.

## 참조

- `docs/design/jira-manual.md`(212~236행, E6-1·E6-2), `docs/design/jira-backlog.md`
  (E6-①·E6-②), `docs/design/session-cache-check.md`(형식·5절 범위 경계),
  `docs/design/schema-strategy.md`(문서 형식 참고), `docs/db/schema.md`
  (`point_balances` 절), `docs/code-convention.md`
