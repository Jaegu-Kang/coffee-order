package com.coffeeorder.order.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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
import com.coffeeorder.menu.entity.Menu;
import com.coffeeorder.menu.repository.MenuRepository;
import com.coffeeorder.order.dto.OrderCreateRequest;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.repository.OrderItemRepository;
import com.coffeeorder.order.repository.OrderRepository;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.entity.PointHistoryType;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.entity.User;
import com.coffeeorder.user.repository.UserRepository;

/**
 * {@link OrderService#order}의 원자성(트랜잭션 경계) 검증(E6-3 태스크1, docs/design/jira-manual.md
 * 246행 "차감+주문 저장 트랜잭션 경계 재점검"). 이 서브태스크는 트랜잭션 경계 재점검 자체(코드
 * 리뷰)와 그 결과를 실증하는 통합 테스트만 다루며, Kafka {@code AFTER_COMMIT} 전환(태스크2)·
 * 롤백 시 미발행 테스트(태스크3, {@code @TransactionalEventListener} 도입 후 별도 서브태스크에서
 * 다룸)·Outbox(태스크4)는 범위 밖이다. 다만 "실패 시 이벤트가 발행되지 않는다"는 현재 구현
 * (커밋 전 동기 발행, {@code docs/api/order.md} 6번 vs 실제 코드 불일치는 이미 인지된 별도
 * 이슈)에서도 자연히 성립하므로 아래 테스트에서 함께 확인한다.
 * <p>
 * 무엇을: 포인트 차감(잔액 갱신 + {@code USE} 이력 insert) 이후 {@code order_items} 저장
 * 단계에서 강제로 예외를 유도해, {@code orders}/{@code order_items} 저장 중 실패가 앞서 수행한
 * 포인트 차감(잔액·이력)까지 포함해 전체 롤백되는지 검증한다. 강제 실패는 프로덕션 코드를
 * 변경하지 않고, 테스트 전용 {@code @Primary} {@link OrderItemRepository} 빈(JDK 동적 프록시로
 * 실제 리포지토리에 위임하되 {@code save(OrderItem)} 호출만 가로채 {@link RuntimeException}을
 * 던짐)으로 격리한다.
 * <p>
 * 왜: {@code docs/api/order.md} "처리(하나의 트랜잭션)" 3~5단계가 실제로 물리적으로 하나의
 * 트랜잭션인지는 {@link OrderServiceConcurrencyTest}(동시성)나 {@link OrderServiceTest}(Mockito
 * 단위, 트랜잭션 프록시 없음)로는 확인되지 않던 공백이다. 두 시나리오를 구분해 검증한다:
 * 기존 사용자(잔액 행 UPDATE 롤백)와 신규 사용자(잔액 행 INSERT 자체가 롤백되어 행이 아예
 * 남지 않아야 함, 계획서에 명시된 빠뜨리기 쉬운 엣지 케이스). 신규 사용자 케이스는 현재 구현상
 * 잔액이 없는 사용자는 기본 잔액 0으로 취급되어 총액이 0보다 크면 항상
 * {@code INSUFFICIENT_POINT}로 먼저 실패하므로({@code balance(0) < totalAmount}), INSERT
 * 분기 자체를 강제 실패 경로까지 도달시키기 위해 가격이 0인 테스트 전용 메뉴를 사용한다
 * ({@code docs/db/schema.md}의 {@code menus.price CHECK(price >= 0)}상 허용되는 값).
 * <p>
 * 어떻게: {@code OrderServiceConcurrencyTest}와 동일하게 {@code @DataJpaTest} +
 * {@code @Import}로 실제 스프링 {@code @Transactional} 프록시를 사용하고,
 * {@code @Transactional(propagation = Propagation.NOT_SUPPORTED)}로 테스트 메서드 자동 롤백을
 * 끈 뒤 {@link TransactionTemplate}으로 커밋을 격리해, "우리가 검증하려는 롤백"과
 * "{@code @DataJpaTest} 기본 롤백"을 혼동하지 않는다(둘을 혼동하면 거짓 양성 위험).
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({JpaAuditingConfig.class, OrderService.class, InMemoryRedisDistributedLockTestConfig.class,
		OrderServiceAtomicityTest.KafkaTemplateTestConfig.class,
		OrderServiceAtomicityTest.FailingOrderItemRepositoryTestConfig.class})
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OrderServiceAtomicityTest {

	private static final Long SEED_MENU_ID = 1L;
	private static final String FORCED_FAILURE_MESSAGE = "강제 실패: order_items 저장 단계(원자성 테스트 전용)";

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
	 * 실제 {@code OrderItemRepository} 빈(Spring Data JPA가 생성한 인터페이스 프록시)을 JDK 동적
	 * 프록시로 감싸 {@code save(OrderItem)} 호출만 가로채고, 그 외 모든 메서드(조회 등)는 그대로
	 * 위임한다. {@code @Primary}로 등록해 {@code OrderService}가 이 대역을 주입받게 하되, 프로덕션
	 * {@code OrderService}/{@code OrderItemRepository} 코드 자체는 전혀 수정하지 않는다.
	 */
	@TestConfiguration
	static class FailingOrderItemRepositoryTestConfig {

		@Bean
		@Primary
		OrderItemRepository failingOrderItemRepository(OrderItemRepository orderItemRepository) {
			InvocationHandler handler = (proxy, method, args) -> {
				if ("save".equals(method.getName()) && args != null && args.length == 1
						&& args[0] instanceof OrderItem) {
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
	private PlatformTransactionManager transactionManager;

	private Long userId;
	private Long zeroPriceMenuId;

	@AfterEach
	void cleanUp() {
		if (userId == null) {
			return;
		}
		Long cleanupUserId = userId;
		Long cleanupMenuId = zeroPriceMenuId;
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
			if (cleanupMenuId != null) {
				menuRepository.findById(cleanupMenuId).ifPresent(menuRepository::delete);
			}
		});
	}

	private long seedMenuPrice() {
		return menuRepository.findById(SEED_MENU_ID).orElseThrow().getPrice();
	}

	private Long createUserWithBalance(long initialBalance) {
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		return transactionTemplate.execute(status -> {
			Long newUserId = userRepository.saveAndFlush(new User("원자성-테스트")).getId();
			pointBalanceRepository.saveAndFlush(new PointBalance(newUserId, initialBalance));
			return newUserId;
		});
	}

	private Long createUserWithoutBalance() {
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		return transactionTemplate.execute(
				status -> userRepository.saveAndFlush(new User("원자성-신규유저-테스트")).getId());
	}

	private Long createZeroPriceMenu() {
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		return transactionTemplate.execute(
				status -> menuRepository.saveAndFlush(new Menu("원자성-테스트-0원메뉴", 0L)).getId());
	}

	private List<PointHistory> useHistoriesOf(Long targetUserId) {
		return pointHistoryRepository.findAll().stream()
				.filter(history -> history.getUserId().equals(targetUserId))
				.filter(history -> history.getType() == PointHistoryType.USE)
				.collect(Collectors.toList());
	}

	private List<Order> ordersOf(Long targetUserId) {
		return orderRepository.findAll().stream()
				.filter(order -> order.getUserId().equals(targetUserId))
				.collect(Collectors.toList());
	}

	@Test
	void 기존_사용자의_주문항목_저장이_실패하면_잔액_차감과_포인트이력과_주문이_모두_롤백된다() {
		long price = seedMenuPrice();
		long initialBalance = price * 3;
		userId = createUserWithBalance(initialBalance);

		int orderItemCountBefore = orderItemRepository.findAll().size();

		assertThatThrownBy(() -> orderService.order(new OrderCreateRequest(userId, SEED_MENU_ID, 1)))
				.isInstanceOf(RuntimeException.class)
				.hasMessage(FORCED_FAILURE_MESSAGE);

		Long balanceAfter = pointBalanceRepository.findById(userId).orElseThrow().getBalance();
		assertThat(balanceAfter).isEqualTo(initialBalance);

		assertThat(useHistoriesOf(userId)).isEmpty();
		assertThat(ordersOf(userId)).isEmpty();
		assertThat(orderItemRepository.findAll()).hasSize(orderItemCountBefore);

		verify(kafkaTemplate, never()).send(any(), any());
	}

	@Test
	void 신규_사용자는_주문항목_저장이_실패하면_잔액행_INSERT_자체가_롤백되어_잔액행이_남지_않는다() {
		zeroPriceMenuId = createZeroPriceMenu();
		userId = createUserWithoutBalance();

		assertThat(pointBalanceRepository.findById(userId)).isEmpty();

		assertThatThrownBy(() -> orderService.order(new OrderCreateRequest(userId, zeroPriceMenuId, 1)))
				.isInstanceOf(RuntimeException.class)
				.hasMessage(FORCED_FAILURE_MESSAGE);

		assertThat(pointBalanceRepository.findById(userId)).isEmpty();
		assertThat(useHistoriesOf(userId)).isEmpty();
		assertThat(ordersOf(userId)).isEmpty();

		verify(kafkaTemplate, never()).send(any(), any());
	}
}
