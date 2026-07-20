package com.coffeeorder.order.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
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
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.event.OrderEvent;
import com.coffeeorder.order.event.OrderEventKafkaListener;
import com.coffeeorder.order.repository.OrderItemRepository;
import com.coffeeorder.order.repository.OrderRepository;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.entity.User;
import com.coffeeorder.user.repository.UserRepository;

/**
 * {@code OrderService.order()}가 실제 스프링 트랜잭션 커밋/롤백과
 * {@link OrderEventKafkaListener}({@code @TransactionalEventListener(phase = AFTER_COMMIT)})를
 * 함께 사용할 때 "커밋 성공 시에만 Kafka 발행되고, 롤백 시에는 구조적으로 발행되지 않는다"를
 * 실제 리스너 배선(wiring)까지 포함해 실증하는 통합 테스트(SCRUM-77 / E6-3 태스크3,
 * docs/design/kafka-after-commit-check.md 6절 "태스크3과의 범위 경계").
 * <p>
 * 무엇을: {@link OrderServiceAtomicityTest}(태스크1)는 {@code kafkaTemplate.send} never-called를
 * 검증하지만 {@link OrderEventKafkaListener}를 {@code @Import}하지 않는다(같은 클래스 Javadoc에
 * 의도적으로 명시). 즉 그 테스트는 "예외가 {@code eventPublisher.publishEvent()} 호출 전에 터져서
 * 애초에 이벤트가 안 만들어짐"만 증명할 뿐, {@code AFTER_COMMIT} 리스너 자체가 롤백 시 호출되지
 * 않는지는 검증하지 못한다. 이 클래스는 그 갭을 메우기 위해 {@link OrderEventKafkaListener}를
 * 실제 스프링 트랜잭션 이벤트 버스에 등록한 상태에서 두 시나리오를 검증한다.
 * <ul>
 * <li>시나리오 A(커밋 성공 → 발행): 정상 주문이 커밋되면 {@code kafkaTemplate.send(order-events,
 * event)}가 정확히 1회 호출되고, 캡처된 {@link OrderEvent}가 실제 저장된 {@code Order}/
 * {@code OrderItem}과 일치한다.</li>
 * <li>시나리오 B(롤백 → 미발행, 이 티켓의 핵심): {@link OrderServiceAtomicityTest}와 동일한 방식
 * (JDK 동적 프록시로 {@code OrderItemRepository.save(OrderItem)}만 가로채 강제
 * {@link RuntimeException})으로 저장 단계를 실패시켜 트랜잭션이 롤백되면, {@code kafkaTemplate.send}가
 * 전혀 호출되지 않는다 — 이번에는 리스너가 실제로 컨텍스트에 등록된 상태에서 확인한다는 점이
 * 태스크1 테스트와의 차별점이다.</li>
 * </ul>
 * 왜: SCRUM-22 AC "결제 롤백 시 잔액 원복 + 이벤트 미발행" 중 "이벤트 미발행"을 리스너 배선까지
 * 포함해 구조적으로 보장됨을 증명하는 것이 목적이다. 잔액 원복 자체는 이미
 * {@link OrderServiceAtomicityTest}가 커버하므로 이 클래스에서는 중복 검증하지 않고 "이벤트
 * 미발행 + 리스너 배선 실증"에 집중한다.
 * <p>
 * 어떻게: {@code @DataJpaTest} + {@code @Import}로 실제 스프링 {@code @Transactional} 프록시와
 * {@code @TransactionalEventListener} 인프라를 함께 사용하고,
 * {@code @Transactional(propagation = Propagation.NOT_SUPPORTED)}로 테스트 메서드 자동 롤백을 꺼
 * "우리가 검증하려는 커밋/롤백"과 "{@code @DataJpaTest} 기본 롤백"을 혼동하지 않는다
 * ({@link OrderServiceAtomicityTest}/{@link OrderServiceConcurrencyTest}와 동일한 함정 회피).
 * 강제 실패는 {@code @Primary} {@link OrderItemRepository} 프록시 빈에 {@link AtomicBoolean} 토글을
 * 두어 시나리오 A/B를 한 클래스(컨텍스트 캐싱 재사용)에서 다루고, {@link #resetForcedFailureAndMock()}
 * 에서 토글과 Mockito mock 호출 이력을 모두 리셋해 테스트 간 누수를 막는다. 프로덕션
 * {@code OrderService}/{@code OrderEventKafkaListener}/{@code OrderEvent}/{@code KafkaProducerConfig}
 * 코드는 전혀 수정하지 않는다.
 * <p>
 * {@code KafkaTemplate}은 실제 Kafka 브로커 없이 Mockito mock 빈으로 대체한다
 * ({@link OrderServiceAtomicityTest}/{@link OrderServiceConcurrencyTest}와 동일 패턴). 동기
 * {@code AFTER_COMMIT} 리스너 내부 실패가 커밋 자체를 되돌리지 못하는 한계, Transactional
 * Outbox(태스크4)는 이 클래스의 범위 밖이다(docs/design/kafka-after-commit-check.md 6절).
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({JpaAuditingConfig.class, OrderService.class, OrderEventKafkaListener.class,
		InMemoryRedisDistributedLockTestConfig.class,
		OrderServiceAfterCommitPublishTest.KafkaTemplateTestConfig.class,
		OrderServiceAfterCommitPublishTest.ToggleableFailingOrderItemRepositoryTestConfig.class})
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OrderServiceAfterCommitPublishTest {

	private static final Long SEED_MENU_ID = 1L;
	private static final String FORCED_FAILURE_MESSAGE = "강제 실패: order_items 저장 단계(AFTER_COMMIT 발행 테스트 전용)";

	/** 실제 Kafka 브로커 없이 {@code KafkaTemplate} 의존성만 채우고, 발행 여부를 검증하기 위한 Mockito mock 빈. */
	@TestConfiguration
	static class KafkaTemplateTestConfig {

		@Bean
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, Object> kafkaTemplate() {
			return mock(KafkaTemplate.class);
		}
	}

	/**
	 * {@code order_items} 저장 단계에서 강제로 예외를 던지는 테스트 전용 {@link OrderItemRepository} 대역.
	 * {@link OrderServiceAtomicityTest}의 프록시와 달리, {@link ForcedFailureToggle}로 실패 유도를
	 * on/off 할 수 있게 해 시나리오 A(정상 커밋)/B(강제 롤백)를 한 클래스(컨텍스트 캐싱)에서 함께
	 * 다룬다. 실제 {@code OrderItemRepository} 빈에는 그대로 위임하고 {@code save(OrderItem)} 호출만
	 * 가로챈다. 프로덕션 {@code OrderService}/{@code OrderItemRepository} 코드는 수정하지 않는다.
	 */
	@TestConfiguration
	static class ToggleableFailingOrderItemRepositoryTestConfig {

		@Bean
		ForcedFailureToggle forcedFailureToggle() {
			return new ForcedFailureToggle();
		}

		@Bean
		@Primary
		OrderItemRepository toggleableFailingOrderItemRepository(OrderItemRepository orderItemRepository,
				ForcedFailureToggle forcedFailureToggle) {
			InvocationHandler handler = (proxy, method, args) -> {
				if (forcedFailureToggle.isEnabled() && "save".equals(method.getName())
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

	/** {@code order_items} 저장 강제 실패 여부를 테스트 메서드별로 켜고 끌 수 있는 토글. */
	static class ForcedFailureToggle {

		private final AtomicBoolean enabled = new AtomicBoolean(false);

		void enable() {
			enabled.set(true);
		}

		void reset() {
			enabled.set(false);
		}

		boolean isEnabled() {
			return enabled.get();
		}
	}

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
	private KafkaTemplate<String, Object> kafkaTemplate;

	@Autowired
	private ForcedFailureToggle forcedFailureToggle;

	@Autowired
	private PlatformTransactionManager transactionManager;

	private Long userId;

	@AfterEach
	void resetForcedFailureAndMock() {
		forcedFailureToggle.reset();
		reset(kafkaTemplate);
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
			Long newUserId = userRepository.saveAndFlush(new User("AFTER_COMMIT-발행-테스트")).getId();
			pointBalanceRepository.saveAndFlush(new PointBalance(newUserId, initialBalance));
			return newUserId;
		});
	}

	@Test
	void 커밋되면_AFTER_COMMIT_리스너가_실제로_호출되어_Kafka로_정확히_1회_발행된다() {
		long price = seedMenuPrice();
		userId = createUserWithBalance(price);

		OrderResult result = orderService.order(new OrderCreateRequest(userId, SEED_MENU_ID, 1));

		Order savedOrder = orderRepository.findById(result.getOrder().getId()).orElseThrow();
		OrderItem savedOrderItem = orderItemRepository.findAll().stream()
				.filter(item -> item.getOrderId().equals(savedOrder.getId()))
				.findFirst()
				.orElseThrow();

		ArgumentCaptor<OrderEvent> eventCaptor = ArgumentCaptor.forClass(OrderEvent.class);
		verify(kafkaTemplate, times(1)).send(eq(KafkaProducerConfig.ORDER_EVENTS_TOPIC), eventCaptor.capture());

		OrderEvent publishedEvent = eventCaptor.getValue();
		assertThat(publishedEvent.getOrderId()).isEqualTo(savedOrder.getId());
		assertThat(publishedEvent.getUserId()).isEqualTo(userId);
		assertThat(publishedEvent.getMenuId()).isEqualTo(savedOrderItem.getMenuId());
		assertThat(publishedEvent.getAmount()).isEqualTo(savedOrder.getTotalAmount());
		assertThat(publishedEvent.getOrderedAt()).isEqualTo(savedOrder.getOrderedAt().toInstant(ZoneOffset.UTC));
	}

	@Test
	void 저장_단계에서_실패해_롤백되면_AFTER_COMMIT_리스너가_호출되지_않아_Kafka로_발행되지_않는다() {
		long price = seedMenuPrice();
		userId = createUserWithBalance(price);
		forcedFailureToggle.enable();

		assertThatThrownBy(() -> orderService.order(new OrderCreateRequest(userId, SEED_MENU_ID, 1)))
				.isInstanceOf(RuntimeException.class)
				.hasMessage(FORCED_FAILURE_MESSAGE);

		verify(kafkaTemplate, never()).send(any(), any());
	}
}
