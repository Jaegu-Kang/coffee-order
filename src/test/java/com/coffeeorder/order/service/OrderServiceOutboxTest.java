package com.coffeeorder.order.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.coffeeorder.common.lock.InMemoryRedisDistributedLockTestConfig;
import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.config.KafkaProducerConfig;
import com.coffeeorder.menu.repository.MenuRepository;
import com.coffeeorder.order.dto.OrderCreateRequest;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.outbox.entity.OutboxEvent;
import com.coffeeorder.order.outbox.entity.OutboxEventStatus;
import com.coffeeorder.order.outbox.repository.OutboxEventRepository;
import com.coffeeorder.order.repository.OrderItemRepository;
import com.coffeeorder.order.repository.OrderRepository;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.entity.User;
import com.coffeeorder.user.repository.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * {@code OrderService.order()}가 실제 스프링 트랜잭션 커밋과 함께 {@code outbox_events}에
 * {@link OutboxEventStatus#PENDING} 행을 원자적으로 남기는지 실증하는 통합 테스트(E6-3 태스크4,
 * SCRUM-78 "(확장) Transactional Outbox 테이블 + 릴레이 설계/구현", 3절 성공 기준 1번).
 * <p>
 * 이 클래스는 SCRUM-77(태스크3)에서 작성했던 {@code OrderServiceAfterCommitPublishTest}를
 * 대체한다. 그 테스트는 "커밋되면 {@code OrderEventKafkaListener}(AFTER_COMMIT)가 실제로 호출되어
 * Kafka로 1회 발행됨"을 검증했지만, SCRUM-78부터 {@code OrderService.order()}는 더 이상
 * {@code ApplicationEventPublisher}/{@code KafkaTemplate}에 의존하지 않고 같은 트랜잭션 안에서
 * {@link OutboxEventRepository}에 {@link OutboxEvent}를 저장하는 것으로 발행 책임을 넘긴다(실제
 * Kafka 발행은 트랜잭션 밖에서 폴링하는 {@code OutboxRelay}가 전담하며 별도 {@code OutboxRelayTest}가
 * 검증한다). 따라서 "커밋 시 발행 트리거가 정상적으로 만들어지는지"를 검증하는 이 계층의 테스트
 * 대상도 "Kafka 리스너 호출"에서 "outbox_events PENDING 행 생성"으로 바뀌었다.
 * <p>
 * "저장 단계 실패(롤백) 시 이벤트가 만들어지지 않는지"(옛 시나리오 B)는 {@link OrderServiceAtomicityTest}가
 * 이미 같은 강제 실패 프록시 패턴으로 {@code outbox_events} 행이 남지 않음을 검증하므로 이 클래스에서
 * 중복하지 않는다({@link OrderServiceAtomicityTest} Javadoc 참고).
 * <p>
 * 어떻게: {@code OrderServiceAtomicityTest}/{@code OrderServiceConcurrencyTest}와 동일하게
 * {@code @DataJpaTest} + {@code @Import}로 실제 스프링 {@code @Transactional} 프록시를 사용하고,
 * {@code @Transactional(propagation = Propagation.NOT_SUPPORTED)}로 테스트 메서드 자동 롤백을 끈
 * 뒤 {@link TransactionTemplate}으로 시드 데이터 커밋/정리를 격리한다. {@code OrderService}가 더
 * 이상 {@code KafkaTemplate}에 의존하지 않으므로 이 테스트는 Kafka 관련 대역(mock)이 필요 없다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({JpaAuditingConfig.class, OrderService.class, InMemoryRedisDistributedLockTestConfig.class})
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OrderServiceOutboxTest {

	private static final Long SEED_MENU_ID = 1L;

	private static final ObjectMapper ORDER_EVENT_OBJECT_MAPPER = KafkaProducerConfig.orderEventObjectMapper();

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
	private PlatformTransactionManager transactionManager;

	private Long userId;

	@AfterEach
	void cleanUp() {
		if (userId == null) {
			return;
		}
		Long cleanupUserId = userId;
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		transactionTemplate.executeWithoutResult(status -> {
			List<Long> orderIds = orderRepository.findAll().stream()
					.filter(order -> order.getUserId().equals(cleanupUserId))
					.map(Order::getId)
					.collect(Collectors.toList());
			orderItemRepository.findAll().stream()
					.filter(item -> orderIds.contains(item.getOrderId()))
					.forEach(orderItemRepository::delete);
			outboxEventRepository.findAll().stream()
					.filter(outboxEvent -> orderIds.contains(outboxEvent.getAggregateId()))
					.forEach(outboxEventRepository::delete);
			orderRepository.findAll().stream()
					.filter(order -> order.getUserId().equals(cleanupUserId))
					.forEach(orderRepository::delete);
			pointHistoryRepository.findAll().stream()
					.filter(history -> history.getUserId().equals(cleanupUserId))
					.forEach(pointHistoryRepository::delete);
			pointBalanceRepository.findById(cleanupUserId).ifPresent(pointBalanceRepository::delete);
			userRepository.findById(cleanupUserId).ifPresent(userRepository::delete);
		});
	}

	private long seedMenuPrice() {
		return menuRepository.findById(SEED_MENU_ID).orElseThrow().getPrice();
	}

	private Long createUserWithBalance(long initialBalance) {
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		return transactionTemplate.execute(status -> {
			Long newUserId = userRepository.saveAndFlush(new User("Outbox-발행-테스트")).getId();
			pointBalanceRepository.saveAndFlush(new PointBalance(newUserId, initialBalance));
			return newUserId;
		});
	}

	@Test
	void 정상_주문이_커밋되면_outbox_events에_PENDING_행이_하나_생성되고_payload가_OrderEvent_스키마와_일치한다() throws Exception {
		long price = seedMenuPrice();
		userId = createUserWithBalance(price);

		OrderResult result = orderService.order(new OrderCreateRequest(userId, SEED_MENU_ID, 1));

		Order savedOrder = orderRepository.findById(result.getOrder().getId()).orElseThrow();

		List<OutboxEvent> outboxEvents = outboxEventRepository.findAll().stream()
				.filter(outboxEvent -> outboxEvent.getAggregateId().equals(savedOrder.getId()))
				.collect(Collectors.toList());
		assertThat(outboxEvents).hasSize(1);

		OutboxEvent outboxEvent = outboxEvents.get(0);
		assertThat(outboxEvent.getAggregateType()).isEqualTo("ORDER");
		assertThat(outboxEvent.getTopic()).isEqualTo(KafkaProducerConfig.ORDER_EVENTS_TOPIC);
		assertThat(outboxEvent.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
		assertThat(outboxEvent.getRetryCount()).isZero();
		assertThat(outboxEvent.getSentAt()).isNull();
		assertThat(outboxEvent.getCreatedAt()).isNotNull();

		@SuppressWarnings("unchecked")
		Map<String, Object> payload = ORDER_EVENT_OBJECT_MAPPER.readValue(outboxEvent.getPayload(), Map.class);
		assertThat(payload.keySet()).containsExactlyInAnyOrder("orderId", "userId", "menuId", "amount", "orderedAt");
		assertThat(((Number) payload.get("orderId")).longValue()).isEqualTo(savedOrder.getId());
		assertThat(((Number) payload.get("userId")).longValue()).isEqualTo(userId);
		assertThat(((Number) payload.get("amount")).longValue()).isEqualTo(savedOrder.getTotalAmount());
		assertThat(Instant.parse((String) payload.get("orderedAt")))
				.isEqualTo(savedOrder.getOrderedAt().toInstant(ZoneOffset.UTC));
	}
}
