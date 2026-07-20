package com.coffeeorder.order.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.common.lock.InMemoryRedisDistributedLockTestConfig;
import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.menu.repository.MenuRepository;
import com.coffeeorder.order.dto.OrderCreateRequest;
import com.coffeeorder.order.entity.Order;
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

/**
 * {@link OrderService#order}의 동시성 테스트(SCRUM-21 AC / SCRUM-74, E6-2 태스크5).
 * <p>
 * 무엇을: 동일 사용자·동일 메뉴에 대해 N개 스레드가 {@link CountDownLatch}로 동시에 출발해
 * {@code order()}를 동시 호출한다. 케이스 A(잔액 = N·가격)는 전부 성공, 케이스 B(잔액 = K·가격,
 * K&lt;N)는 정확히 K건만 성공하고 나머지는 {@code INSUFFICIENT_POINT}로 실패하는 시나리오를
 * 검증한다.
 * <p>
 * 왜: "동일 사용자 동시 충전/결제 N건 → 분실 갱신 없음, 최종 잔액=기대값" AC를 결제(차감) 경로에
 * 대해서도 증명하기 위함이다. 특히 케이스 B는 오버셀(성공 건수가 실제 잔액으로 감당 가능한
 * 건수를 초과)·음수 잔액이 발생하지 않음을 함께 확인한다. 이는 기존 Mockito 단위
 * 테스트({@link OrderServiceTest}, 락을 즉시 통과하도록 스텁)로는 커버되지 않던 공백이다.
 * <p>
 * 어떻게: {@code com.coffeeorder.point.service.PointServiceConcurrencyTest}와 동일하게
 * {@link com.coffeeorder.common.lock.InMemoryRedisDistributedLock} 대역을 사용해 실제 Redis
 * 없이 결정적으로 검증한다(다중 인스턴스 간 실제 Redis 분산락 자체의 검증은 범위 밖 — 해당
 * 클래스 Javadoc 참고). 발행-트랜잭션 정합성(Outbox, SCRUM-78)은 별도 E6-3 태스크4 범위이며,
 * {@code OrderService}는 {@code outbox_events} 저장에 {@code KafkaTemplate}을 필요로 하지
 * 않으므로(실제 Kafka 발행은 별도 {@code OutboxRelay} 담당) 이 테스트도 Kafka 관련 대역이
 * 필요 없다. {@code OrderService}를 {@code @Import}로 등록해 Spring이 관리하는 실제
 * {@code @Transactional} 프록시를 사용한다(수동 {@code new OrderService(...)}는 트랜잭션
 * 프록시가 없어 비관적 락이 조기 해제되므로 회피).
 * <p>
 * 시드 메뉴 {@code id=1}(가격은 {@link MenuRepository}로 조회해 매직 넘버 중복을 피함)을 사용하고,
 * 시드 사용자(user_id 1~3)와 무관한 신규 사용자를 생성해 격리한다. {@code PointBalanceRepositoryLockTest}
 * 선례와 동일하게 스레드별 별도 커밋 트랜잭션이 필요하므로 {@code @DataJpaTest}의 테스트-메서드
 * 트랜잭션 래핑을 {@code @Transactional(propagation = Propagation.NOT_SUPPORTED)}로 끄고,
 * {@link #cleanUp()}에서 생성한 행을 정리한다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({JpaAuditingConfig.class, OrderService.class, InMemoryRedisDistributedLockTestConfig.class})
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OrderServiceConcurrencyTest {

	private static final Long SEED_MENU_ID = 1L;
	private static final long AWAIT_TIMEOUT_SECONDS = 30L;

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
			Long newUserId = userRepository.saveAndFlush(new User("동시성-결제-테스트")).getId();
			pointBalanceRepository.saveAndFlush(new PointBalance(newUserId, initialBalance));
			return newUserId;
		});
	}

	@Test
	void 잔액이_정확히_소진되는_N건_동시_주문은_전부_성공하고_최종_잔액은_0이_된다() throws Exception {
		int threadCount = 20;
		long price = seedMenuPrice();
		userId = createUserWithBalance(threadCount * price);

		ConcurrentOrderRunResult runResult = runConcurrentOrders(threadCount, userId);

		assertThat(runResult.otherFailures).isEmpty();
		assertThat(runResult.insufficientPointFailureCount.get()).isZero();
		assertThat(runResult.successResults).hasSize(threadCount);

		Long finalBalance = pointBalanceRepository.findById(userId).orElseThrow().getBalance();
		assertThat(finalBalance).isZero();

		List<Order> savedOrders = orderRepository.findAll().stream()
				.filter(order -> order.getUserId().equals(userId))
				.collect(Collectors.toList());
		assertThat(savedOrders).hasSize(threadCount);

		assertThat(useHistoriesOf(userId)).hasSize(threadCount);
	}

	@Test
	void 잔액이_부족한_상태에서_N건_동시_주문하면_정확히_K건만_성공하고_최종_잔액은_0으로_수렴한다() throws Exception {
		int threadCount = 20;
		int expectedSuccessCount = 7;
		long price = seedMenuPrice();
		userId = createUserWithBalance(expectedSuccessCount * price);

		ConcurrentOrderRunResult runResult = runConcurrentOrders(threadCount, userId);

		assertThat(runResult.otherFailures).isEmpty();
		assertThat(runResult.successResults).hasSize(expectedSuccessCount);
		assertThat(runResult.insufficientPointFailureCount.get()).isEqualTo(threadCount - expectedSuccessCount);

		Long finalBalance = pointBalanceRepository.findById(userId).orElseThrow().getBalance();
		assertThat(finalBalance).isZero();
		assertThat(finalBalance).isNotNegative();

		List<Order> savedOrders = orderRepository.findAll().stream()
				.filter(order -> order.getUserId().equals(userId))
				.collect(Collectors.toList());
		assertThat(savedOrders).hasSize(expectedSuccessCount);

		assertThat(useHistoriesOf(userId)).hasSize(expectedSuccessCount);
	}

	private List<PointHistory> useHistoriesOf(Long targetUserId) {
		return pointHistoryRepository.findAll().stream()
				.filter(history -> history.getUserId().equals(targetUserId))
				.filter(history -> history.getType() == PointHistoryType.USE)
				.collect(Collectors.toList());
	}

	/**
	 * {@code threadCount}개 스레드로 동일 사용자·시드 메뉴에 대한 {@code order()}를
	 * {@link CountDownLatch}로 동시 출발시키고, 성공 결과와 {@code INSUFFICIENT_POINT} 실패 건수를
	 * 분리해 집계한다. {@code INSUFFICIENT_POINT} 외의 예외는 {@code otherFailures}에 담아 테스트가
	 * 이를 명시적으로 비어 있음을 검증하게 한다(예상치 못한 실패를 조용히 흡수하지 않기 위함).
	 */
	private ConcurrentOrderRunResult runConcurrentOrders(int threadCount, Long targetUserId) throws Exception {
		CountDownLatch readyLatch = new CountDownLatch(threadCount);
		CountDownLatch startLatch = new CountDownLatch(1);
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		AtomicInteger insufficientPointFailureCount = new AtomicInteger();
		List<Throwable> otherFailures = new ArrayList<>();
		List<OrderResult> successResults = new ArrayList<>();

		try {
			List<Callable<OrderResult>> orderTasks = IntStream.range(0, threadCount)
					.mapToObj(i -> (Callable<OrderResult>) () -> {
						readyLatch.countDown();
						startLatch.await();
						return orderService.order(new OrderCreateRequest(targetUserId, SEED_MENU_ID, 1));
					})
					.collect(Collectors.toList());

			List<Future<OrderResult>> futures = orderTasks.stream()
					.map(executor::submit)
					.collect(Collectors.toList());

			assertThat(readyLatch.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
			startLatch.countDown();

			for (Future<OrderResult> future : futures) {
				try {
					successResults.add(future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
				} catch (ExecutionException e) {
					Throwable cause = e.getCause();
					if (cause instanceof BusinessException businessException
							&& businessException.getErrorCode() == ErrorCode.INSUFFICIENT_POINT) {
						insufficientPointFailureCount.incrementAndGet();
					} else {
						otherFailures.add(cause);
					}
				}
			}
		} finally {
			executor.shutdown();
		}

		return new ConcurrentOrderRunResult(successResults, insufficientPointFailureCount, otherFailures);
	}

	/** {@link #runConcurrentOrders}의 집계 결과(성공/INSUFFICIENT_POINT 실패/그 외 실패)를 담는 값 객체. */
	private static class ConcurrentOrderRunResult {

		private final List<OrderResult> successResults;
		private final AtomicInteger insufficientPointFailureCount;
		private final List<Throwable> otherFailures;

		private ConcurrentOrderRunResult(List<OrderResult> successResults, AtomicInteger insufficientPointFailureCount,
				List<Throwable> otherFailures) {
			this.successResults = successResults;
			this.insufficientPointFailureCount = insufficientPointFailureCount;
			this.otherFailures = otherFailures;
		}
	}
}
