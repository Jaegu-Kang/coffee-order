package com.coffeeorder.order.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mysql.MySQLContainer;

import com.coffeeorder.common.lock.RedisDistributedLock;
import com.coffeeorder.config.KafkaProducerConfig;
import com.coffeeorder.menu.repository.MenuRepository;
import com.coffeeorder.order.dto.OrderCreateRequest;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.entity.OrderStatus;
import com.coffeeorder.order.outbox.OutboxRelay;
import com.coffeeorder.order.outbox.entity.OutboxEvent;
import com.coffeeorder.order.outbox.entity.OutboxEventStatus;
import com.coffeeorder.order.outbox.repository.OutboxEventRepository;
import com.coffeeorder.order.repository.OrderItemRepository;
import com.coffeeorder.order.repository.OrderRepository;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.entity.PointHistoryType;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.entity.User;
import com.coffeeorder.user.repository.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.testcontainers.RedisContainer;

/**
 * {@code OrderService}(주문/결제) 흐름을 가짜(H2/EmbeddedKafka/{@code InMemoryRedisDistributedLock})가
 * 아닌 실제 MySQL·Redis·Kafka 컨테이너(Testcontainers) 위에서 end-to-end로 검증하는 통합 테스트(SCRUM-23).
 * <p>
 * <b>왜</b>: 기존 {@link OrderServiceOutboxTest}/{@link OrderServiceAtomicityTest}/{@link OrderServiceConcurrencyTest}는
 * {@code @DataJpaTest} + H2 + {@link com.coffeeorder.common.lock.InMemoryRedisDistributedLock} 대역으로
 * 서비스 로직 자체는 충분히 검증하지만, 실제 MySQL 방언·실제 Redis 분산락(SET NX PX/Lua 해제
 * 스크립트)·실제 Kafka 프로듀서/브로커 프로토콜 위에서 컨트롤러→서비스→리포지토리 전체 배선이 동작하는지는
 * 검증하지 않는다({@link com.coffeeorder.common.lock.InMemoryRedisDistributedLock} Javadoc의
 * "이 대역이 메우지 못하는 공백" 참고). 이 클래스가 그 공백을 메운다.
 * <p>
 * <b>어떻게</b>: {@link Testcontainers}({@code disabledWithoutDocker = true})로 Docker가 없는 환경에서는
 * 이 클래스 전체가 스킵되어 README §7.1 "인프라 없이 {@code ./gradlew test} 통과" 불변식을 깨지 않는다.
 * {@code docker-compose.yml}과 동일한 이미지 태그(mysql:8.0, apache/kafka:3.7.0, redis:7-alpine)로
 * 컨테이너 3개를 띄우고, {@link DynamicPropertySource}로 데이터소스/Kafka/Redis 접속 정보를 실제
 * 컨테이너 값으로 덮어써 {@code application-test.yml}의 H2 설정 대신 실제 인프라에 연결한다. 어떤
 * 대역(fake)도 {@code @Import}하지 않는다 — 이 테스트의 존재 이유 자체가 "진짜"이기 때문이다.
 * {@code @ActiveProfiles("test")}는 유지해 {@code SchedulingConfig}/{@code KafkaProducerConfig}의
 * {@code kafkaAdmin}/{@code orderEventsTopic} 빈(둘 다 {@code @Profile("!test")})은 여전히
 * 비활성화되므로, 토픽 존재는 {@link #createOrderEventsTopicIfAbsent()}에서 이 테스트가 직접
 * {@link AdminClient}로 명시적으로 보장한다(브로커 auto-create 설정에 우연히 의존하지 않기 위함).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("test")
@Testcontainers(disabledWithoutDocker = true)
class OrderServiceTestcontainersIntegrationTest {

	private static final Long SEED_MENU_ID = 1L;

	private static final String CONSUMER_GROUP_ID = "order-events-testcontainers-integration-test";

	private static final long KAFKA_POLL_TIMEOUT_SECONDS = 15L;

	private static final ObjectMapper ORDER_EVENT_OBJECT_MAPPER = KafkaProducerConfig.orderEventObjectMapper();

	private static final String FORCED_FAILURE_MESSAGE =
			"강제 실패: order_items 저장 단계(Testcontainers 통합 테스트 전용)";

	private static final long FORCED_FAILURE_KAFKA_POLL_TIMEOUT_SECONDS = 5L;

	/**
	 * {@code order_items} 저장 단계를 강제 실패시킬지 여부를 나타내는 스위치({@code OrderServiceAtomicityTest}의
	 * 프록시 패턴 참고). 기본값 OFF로 두어 기존 두 개의 end-to-end 테스트에는 전혀 영향을 주지 않고,
	 * 강제 실패 테스트에서만 켠 뒤 {@link #resetForcedFailureToggle()}에서 매 테스트 종료 후 되돌린다.
	 * {@link ForcedFailureOrderItemRepositoryTestConfig}가 등록하는 {@code @Primary} 빈은 스프링
	 * 컨테이너 내에서 싱글턴이라 테스트 메서드마다 새로 생성되는 인스턴스 필드로는 이 스위치를 공유할 수
	 * 없으므로 static으로 둔다.
	 */
	private static final AtomicBoolean FORCE_ORDER_ITEM_SAVE_FAILURE = new AtomicBoolean(false);

	/**
	 * {@code OrderServiceAtomicityTest.FailingOrderItemRepositoryTestConfig}와 동일한 방식(JDK
	 * 동적 프록시로 실제 {@code OrderItemRepository}에 위임하되 {@code save(OrderItem)} 호출만 가로챔)이되,
	 * {@link #FORCE_ORDER_ITEM_SAVE_FAILURE}가 켜져 있을 때만 예외를 던진다. {@code @SpringBootTest}가
	 * 로드하는 테스트 클래스에 static 중첩 {@code @TestConfiguration}을 두면 별도의 {@code @Import} 없이도
	 * 스프링 부트가 자동으로 인식해 기본 애플리케이션 설정에 추가로 적용한다. 프로덕션
	 * {@code OrderService}/{@code OrderItemRepository} 코드는 전혀 수정하지 않는다.
	 */
	@TestConfiguration
	static class ForcedFailureOrderItemRepositoryTestConfig {

		@Bean
		@Primary
		OrderItemRepository forcedFailureOrderItemRepository(OrderItemRepository orderItemRepository) {
			InvocationHandler handler = (proxy, method, args) -> {
				if (FORCE_ORDER_ITEM_SAVE_FAILURE.get() && "save".equals(method.getName())
						&& args != null && args.length == 1 && args[0] instanceof OrderItem) {
					throw new RuntimeException(FORCED_FAILURE_MESSAGE);
				}
				try {
					return method.invoke(orderItemRepository, args);
				} catch (InvocationTargetException e) {
					throw e.getCause();
				}
			};
			return (OrderItemRepository) Proxy.newProxyInstance(
					OrderItemRepository.class.getClassLoader(),
					new Class<?>[] {OrderItemRepository.class},
					handler);
		}
	}

	@Container
	static final MySQLContainer MYSQL_CONTAINER = new MySQLContainer("mysql:8.0")
			.withDatabaseName("coffee_order")
			.withUsername("coffee")
			.withPassword("coffee");

	@Container
	static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer("apache/kafka:3.7.0");

	@Container
	static final RedisContainer REDIS_CONTAINER = new RedisContainer("redis:7-alpine");

	@DynamicPropertySource
	static void registerContainerProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", MYSQL_CONTAINER::getJdbcUrl);
		registry.add("spring.datasource.username", MYSQL_CONTAINER::getUsername);
		registry.add("spring.datasource.password", MYSQL_CONTAINER::getPassword);
		registry.add("spring.datasource.driver-class-name", MYSQL_CONTAINER::getDriverClassName);
		registry.add("spring.kafka.bootstrap-servers", KAFKA_CONTAINER::getBootstrapServers);
		registry.add("spring.data.redis.host", REDIS_CONTAINER::getRedisHost);
		registry.add("spring.data.redis.port", REDIS_CONTAINER::getRedisPort);
	}

	/**
	 * {@code test} 프로파일에서는 {@link KafkaProducerConfig#orderEventsTopic()}/{@code kafkaAdmin()}이
	 * 비활성화되므로({@code @Profile("!test")}), {@code order-events} 토픽 존재를 브로커의
	 * auto-create 설정에 맡기지 않고 이 테스트가 직접 명시적으로 생성해 결정성을 확보한다(클래스
	 * Javadoc 참고). 컨테이너는 {@link Testcontainers} 확장의 {@code beforeAll}이 먼저 기동시키므로
	 * 이 시점에는 이미 부트스트랩 서버에 접속할 수 있다.
	 */
	@BeforeAll
	static void createOrderEventsTopicIfAbsent() throws Exception {
		Map<String, Object> adminConfig = new HashMap<>();
		adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
		try (AdminClient adminClient = AdminClient.create(adminConfig)) {
			adminClient.createTopics(List.of(new NewTopic(KafkaProducerConfig.ORDER_EVENTS_TOPIC, 1, (short) 1)))
					.all()
					.get(10, TimeUnit.SECONDS);
		} catch (Exception e) {
			if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
				throw e;
			}
		}
	}

	@Autowired
	private TestRestTemplate testRestTemplate;

	@Autowired
	private OrderService orderService;

	@Autowired
	private UserRepository userRepository;

	@Autowired
	private MenuRepository menuRepository;

	@Autowired
	private PointBalanceRepository pointBalanceRepository;

	@Autowired
	private PointHistoryRepository pointHistoryRepository;

	@Autowired
	private OrderRepository orderRepository;

	@Autowired
	private OrderItemRepository orderItemRepository;

	@Autowired
	private OutboxEventRepository outboxEventRepository;

	@Autowired
	private OutboxRelay outboxRelay;

	@Autowired
	private RedisDistributedLock redisDistributedLock;

	@Autowired
	private RedisTemplate<String, Object> redisTemplate;

	@AfterEach
	void resetForcedFailureToggle() {
		FORCE_ORDER_ITEM_SAVE_FAILURE.set(false);
	}

	private Long createUserWithBalance(long initialBalance) {
		Long newUserId = userRepository.save(new User("Testcontainers-통합-테스트")).getId();
		pointBalanceRepository.save(new PointBalance(newUserId, initialBalance));
		return newUserId;
	}

	@Test
	void 실제_MySQL_Redis_Kafka_컨테이너_위에서_주문_결제부터_Outbox_릴레이_Kafka_발행까지_end_to_end로_동작한다() throws Exception {
		long price = menuRepository.findById(SEED_MENU_ID).orElseThrow().getPrice();
		Long userId = createUserWithBalance(price);

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		String requestBody = String.format("{\"userId\":%d,\"menuId\":%d,\"quantity\":1}", userId, SEED_MENU_ID);
		HttpEntity<String> requestEntity = new HttpEntity<>(requestBody, headers);

		@SuppressWarnings("unchecked")
		ResponseEntity<Map<String, Object>> response = (ResponseEntity<Map<String, Object>>) (ResponseEntity<?>)
				testRestTemplate.postForEntity("/api/orders", requestEntity, Map.class);

		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
		Map<String, Object> body = response.getBody();
		assertThat(body).isNotNull();
		assertThat(body.keySet()).containsExactlyInAnyOrder(
				"orderId", "userId", "items", "totalAmount", "balanceAfter", "status");
		assertThat(((Number) body.get("userId")).longValue()).isEqualTo(userId);
		assertThat(((Number) body.get("totalAmount")).longValue()).isEqualTo(price);
		assertThat(((Number) body.get("balanceAfter")).longValue()).isZero();
		assertThat(body.get("status")).isEqualTo("PAID");

		Long orderId = ((Number) body.get("orderId")).longValue();

		// 1) 실제 MySQL에 저장된 행 검증(orders/order_items/point_balances/point_histories/outbox_events).
		Order savedOrder = orderRepository.findById(orderId).orElseThrow();
		assertThat(savedOrder.getUserId()).isEqualTo(userId);
		assertThat(savedOrder.getTotalAmount()).isEqualTo(price);
		assertThat(savedOrder.getStatus()).isEqualTo(OrderStatus.PAID);

		List<OrderItem> savedOrderItems = orderItemRepository.findAll().stream()
				.filter(item -> item.getOrderId().equals(orderId))
				.collect(Collectors.toList());
		assertThat(savedOrderItems).hasSize(1);
		assertThat(savedOrderItems.get(0).getMenuId()).isEqualTo(SEED_MENU_ID);
		assertThat(savedOrderItems.get(0).getUnitPrice()).isEqualTo(price);

		PointBalance savedBalance = pointBalanceRepository.findById(userId).orElseThrow();
		assertThat(savedBalance.getBalance()).isZero();

		List<PointHistory> savedHistories = pointHistoryRepository.findAll().stream()
				.filter(history -> history.getUserId().equals(userId))
				.collect(Collectors.toList());
		assertThat(savedHistories).hasSize(1);
		assertThat(savedHistories.get(0).getType()).isEqualTo(PointHistoryType.USE);
		assertThat(savedHistories.get(0).getAmount()).isEqualTo(price);
		assertThat(savedHistories.get(0).getBalanceAfter()).isZero();

		List<OutboxEvent> savedOutboxEvents = outboxEventRepository.findAll().stream()
				.filter(event -> event.getAggregateId().equals(orderId))
				.collect(Collectors.toList());
		assertThat(savedOutboxEvents).hasSize(1);
		OutboxEvent outboxEvent = savedOutboxEvents.get(0);
		assertThat(outboxEvent.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
		assertThat(outboxEvent.getTopic()).isEqualTo(KafkaProducerConfig.ORDER_EVENTS_TOPIC);

		// 2) 진짜 Redis(outbox-relay 락)와 진짜 Kafka(KafkaTemplate)를 실제로 경유해 발행되는지 확인.
		outboxRelay.relayPendingEvents();

		OutboxEvent relayedOutboxEvent = outboxEventRepository.findById(outboxEvent.getId()).orElseThrow();
		assertThat(relayedOutboxEvent.getStatus()).isEqualTo(OutboxEventStatus.SENT);
		assertThat(relayedOutboxEvent.getSentAt()).isNotNull();

		// 3) 실제 KafkaConsumer로 order-events를 구독해 발행된 메시지가 명세와 일치하는지 확인.
		ConsumerRecord<String, String> record = pollForOrderEvent(orderId);
		Map<String, Object> payload = ORDER_EVENT_OBJECT_MAPPER.readValue(record.value(), Map.class);
		assertThat(payload.keySet()).containsExactlyInAnyOrder("orderId", "userId", "menuId", "amount", "orderedAt");
		assertThat(((Number) payload.get("orderId")).longValue()).isEqualTo(orderId);
		assertThat(((Number) payload.get("userId")).longValue()).isEqualTo(userId);
		assertThat(((Number) payload.get("menuId")).longValue()).isEqualTo(SEED_MENU_ID);
		assertThat(((Number) payload.get("amount")).longValue()).isEqualTo(price);
		String orderedAt = (String) payload.get("orderedAt");
		assertThat(orderedAt).endsWith("Z");
		assertThat(Instant.parse(orderedAt)).isEqualTo(savedOrder.getOrderedAt().toInstant(ZoneOffset.UTC));
	}

	@Test
	void RedisDistributedLock이_실제_Redis_컨테이너에_point_userId_키를_SET하고_콜백_종료_후_해제한다() {
		String lockKey = "point:redis-proof-" + System.nanoTime();

		redisDistributedLock.executeWithLock(lockKey, Duration.ofSeconds(3), Duration.ofSeconds(5), () -> {
			assertThat(redisTemplate.hasKey(lockKey)).isTrue();
			return null;
		});

		assertThat(redisTemplate.hasKey(lockKey)).isFalse();
	}

	/**
	 * {@code OrderServiceAtomicityTest}가 H2 위에서 검증한 "저장 단계 강제 실패 시 롤백" 시나리오를, 실제
	 * MySQL/Redis/Kafka 위에서도 성립하는지 확인한다(태스크3 범위: {@code OrderServiceAtomicityTest}
	 * 클래스 Javadoc의 "롤백 시 미발행 테스트"). {@link #FORCE_ORDER_ITEM_SAVE_FAILURE}를 켠 채
	 * {@code order_items} 저장 단계에서 강제로 예외를 유도해, 앞서 수행한 포인트 차감(잔액·이력)까지
	 * 포함한 전체 트랜잭션이 실제 MySQL 위에서 물리적으로 롤백되는지, {@code outbox_events}에도 행이
	 * 남지 않는지(커밋 전 저장이므로 애초에 insert 자체가 롤백), {@code outboxRelay.relayPendingEvents()}를
	 * 호출해도({@code outbox_events}가 비어 있으므로) 아무 변화가 없는지, 마지막으로 실제 Kafka
	 * 컨슈머로 5초간 폴링해도 이 사용자에 대한 이벤트가 전혀 수신되지 않는지를 함께 검증한다.
	 */
	@Test
	void 저장_단계_강제_실패_시_실제_인프라_위에서_트랜잭션이_롤백되고_outbox_Kafka로_이벤트가_전혀_발행되지_않는다() throws Exception {
		long price = menuRepository.findById(SEED_MENU_ID).orElseThrow().getPrice();
		long initialBalance = price * 3;
		Long userId = createUserWithBalance(initialBalance);

		int outboxEventCountBefore = outboxEventRepository.findAll().size();

		FORCE_ORDER_ITEM_SAVE_FAILURE.set(true);
		assertThatThrownBy(() -> orderService.order(new OrderCreateRequest(userId, SEED_MENU_ID, 1)))
				.isInstanceOf(RuntimeException.class)
				.hasMessage(FORCED_FAILURE_MESSAGE);

		PointBalance balanceAfter = pointBalanceRepository.findById(userId).orElseThrow();
		assertThat(balanceAfter.getBalance()).isEqualTo(initialBalance);

		List<PointHistory> useHistories = pointHistoryRepository.findAll().stream()
				.filter(history -> history.getUserId().equals(userId))
				.filter(history -> history.getType() == PointHistoryType.USE)
				.collect(Collectors.toList());
		assertThat(useHistories).isEmpty();

		List<Order> orders = orderRepository.findAll().stream()
				.filter(order -> order.getUserId().equals(userId))
				.collect(Collectors.toList());
		assertThat(orders).isEmpty();

		assertThat(outboxEventRepository.findAll()).hasSize(outboxEventCountBefore);

		// relay를 호출해도(발행 대상 outbox 행 자체가 없으므로) outbox_events 개수는 그대로다.
		outboxRelay.relayPendingEvents();
		assertThat(outboxEventRepository.findAll()).hasSize(outboxEventCountBefore);

		assertNoOrderEventPublishedForUser(userId, Duration.ofSeconds(FORCED_FAILURE_KAFKA_POLL_TIMEOUT_SECONDS));
	}

	/**
	 * {@code order-events} 토픽을 처음부터({@code earliest}) 구독해, {@code orderId}에 해당하는
	 * 레코드를 찾을 때까지 폴링한다. 이 테스트 클래스 안에서 여러 {@code @Test}가 같은 토픽을
	 * 공유하므로(컨테이너가 클래스 단위로 재사용됨) 단순히 첫 레코드만 확인하지 않고 {@code orderId}로
	 * 필터링한다.
	 */
	private ConsumerRecord<String, String> pollForOrderEvent(Long orderId) throws Exception {
		Map<String, Object> consumerConfig = new HashMap<>();
		consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
		consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID + "-" + orderId);
		consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(),
				new StringDeserializer())) {
			consumer.subscribe(List.of(KafkaProducerConfig.ORDER_EVENTS_TOPIC));

			long deadline = System.currentTimeMillis() + Duration.ofSeconds(KAFKA_POLL_TIMEOUT_SECONDS).toMillis();
			while (System.currentTimeMillis() < deadline) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> payload = ORDER_EVENT_OBJECT_MAPPER.readValue(record.value(), Map.class);
					if (((Number) payload.get("orderId")).longValue() == orderId) {
						return record;
					}
				}
			}
		}
		throw new AssertionError("order-events 토픽에서 orderId=" + orderId + " 레코드를 시간 내에 수신하지 못했습니다.");
	}

	/**
	 * 강제 실패 시나리오에서는 주문 자체가 롤백되어 {@code orderId}가 존재하지 않으므로 {@code userId}로
	 * 이벤트를 식별한다. 이 테스트 클래스의 각 {@code @Test}는 매번 새 사용자를 생성해 {@code userId}가
	 * 서로 겹치지 않으므로(같은 토픽을 공유하더라도) 다른 테스트가 발행한 이벤트와 혼동되지 않는다.
	 * {@code timeout} 동안 폴링해 해당 {@code userId}의 이벤트가 하나라도 수신되면 즉시 실패시킨다.
	 */
	private void assertNoOrderEventPublishedForUser(Long userId, Duration timeout) throws Exception {
		Map<String, Object> consumerConfig = new HashMap<>();
		consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
		consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID + "-forced-failure-" + userId);
		consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(),
				new StringDeserializer())) {
			consumer.subscribe(List.of(KafkaProducerConfig.ORDER_EVENTS_TOPIC));

			long deadline = System.currentTimeMillis() + timeout.toMillis();
			while (System.currentTimeMillis() < deadline) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> payload = ORDER_EVENT_OBJECT_MAPPER.readValue(record.value(), Map.class);
					if (((Number) payload.get("userId")).longValue() == userId) {
						throw new AssertionError("강제 실패 이후에도 order-events 토픽에 userId=" + userId
								+ " 이벤트가 발행되었습니다: " + record.value());
					}
				}
			}
		}
	}
}
