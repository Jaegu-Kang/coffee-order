package com.coffeeorder.order.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
import java.util.stream.Stream;

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
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.outbox.repository.OutboxEventRepository;
import com.coffeeorder.order.repository.OrderItemRepository;
import com.coffeeorder.order.repository.OrderRepository;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.entity.PointHistoryType;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.point.service.PointService;
import com.coffeeorder.user.entity.User;
import com.coffeeorder.user.repository.UserRepository;

/**
 * {@code point:{userId}} 락 경로를 공유하는 {@link PointService#charge}와 {@link OrderService#order}를
 * 동시에(mixed) 실행하는 동시성 테스트(SCRUM-82, E6-4/SCRUM-23 태스크4 "동시성 테스트(동시 충전·결제)").
 * <p>
 * 무엇을: 동일 사용자 1명에 대해 충전 스레드 {@value #CHARGE_THREAD_COUNT}개와 결제(주문) 스레드
 * {@value #ORDER_THREAD_COUNT}개를 {@link CountDownLatch}로 동시에 출발시켜 뒤섞어 호출한다.
 * <p>
 * 왜: {@link com.coffeeorder.point.service.PointServiceConcurrencyTest}(충전만 N-스레드)와
 * {@link OrderServiceConcurrencyTest}(결제만 N-스레드)는 각각 동종(homogeneous) 연산만 검증하며,
 * "충전과 결제가 같은 사용자에 대해 동시에 섞여 들어오는" 시나리오는 커버하지 못한다. 두 서비스 모두
 * 동일한 {@code point:{userId}} 락 키 프리픽스를 공유하므로, 두 연산이 실제로 상호 배제되어 분실
 * 갱신·오버셀·잔액 불일치가 없는지 이 테스트에서 함께 증명한다.
 * <p>
 * 어떻게: 순서가 비결정적(락 획득 순서는 보장되지 않음)이므로 특정 실행 순서를 가정하는 단언은 사용하지
 * 않는다. 대신 "성공한 충전/결제 건수"를 실제 관찰값으로 집계한 뒤, 최종 잔액·이력 건수·저장된
 * 주문/주문항목 건수가 그 집계값과 정합적인지(합계·건수 기반)만 검증한다. 결제 실패는
 * {@code INSUFFICIENT_POINT}(409)만 허용하며, 그 외 예외(특히 락 관련 {@code CONCURRENCY_CONFLICT})는
 * 0건이어야 한다({@code otherFailures}로 명시적으로 비어 있음을 단언).
 * <p>
 * 자매 테스트들과 동일하게 {@link com.coffeeorder.common.lock.InMemoryRedisDistributedLock}
 * 대역(계약은 동일: 키별 상호배제 + waitTime 내 미획득 시 {@code CONCURRENCY_CONFLICT})을 사용하고,
 * {@code PointService}/{@code OrderService}를 {@code @Import}로 등록해 Spring이 관리하는 실제
 * {@code @Transactional} 프록시를 사용한다(수동 {@code new}는 비관적 락이 조기 해제되므로 회피).
 * {@code @DataJpaTest}의 테스트-메서드 트랜잭션 래핑은 스레드별 별도 커밋 트랜잭션이 필요하므로
 * {@code @Transactional(propagation = Propagation.NOT_SUPPORTED)}로 끄고, 시드 사용자(user_id 1~3)와
 * 무관한 신규 사용자를 생성해 {@link #cleanUp()}에서 정리한다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({JpaAuditingConfig.class, PointService.class, OrderService.class, InMemoryRedisDistributedLockTestConfig.class})
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class PointChargeAndOrderPaymentConcurrencyTest {

	private static final Long SEED_MENU_ID = 1L;
	private static final int CHARGE_THREAD_COUNT = 10;
	private static final int ORDER_THREAD_COUNT = 10;
	private static final long CHARGE_AMOUNT = 1000L;
	private static final long AWAIT_TIMEOUT_SECONDS = 30L;

	@Autowired
	private PointService pointService;

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
			Long newUserId = userRepository.saveAndFlush(new User("동시성-충전결제-혼합-테스트")).getId();
			pointBalanceRepository.saveAndFlush(new PointBalance(newUserId, initialBalance));
			return newUserId;
		});
	}

	@Test
	void 동일_사용자에_대한_동시_충전과_결제가_뒤섞여도_분실_갱신_없이_잔액과_이력이_정합적이다() throws Exception {
		long price = seedMenuPrice();
		long initialBalance = price * 3;
		userId = createUserWithBalance(initialBalance);

		MixedRunResult runResult = runConcurrentChargeAndOrder(userId, price);

		// 성공 기준 3: 결제 실패는 오직 INSUFFICIENT_POINT만 허용, 그 외 예외(락 충돌 등)는 0건.
		assertThat(runResult.otherFailures).isEmpty();
		// charge()는 잔액 부족으로 실패하지 않으므로, 예외가 없다면 전부 성공해야 한다.
		assertThat(runResult.chargeSuccessCount).isEqualTo(CHARGE_THREAD_COUNT);
		assertThat(runResult.insufficientPointFailureCount)
				.isEqualTo(ORDER_THREAD_COUNT - runResult.orderSuccessCount);

		// 성공 기준 1: 최종 잔액 = 초기 잔액 + 성공한 충전 합계 - 성공한 결제 합계, 음수 아님.
		long expectedFinalBalance = initialBalance
				+ (long) runResult.chargeSuccessCount * CHARGE_AMOUNT
				- (long) runResult.orderSuccessCount * price;
		Long finalBalance = pointBalanceRepository.findById(userId).orElseThrow().getBalance();
		assertThat(finalBalance).isEqualTo(expectedFinalBalance);
		assertThat(finalBalance).isNotNegative();

		// 성공 기준 2: point_histories의 CHARGE/USE 건수가 성공 건수와 일치.
		List<PointHistory> histories = pointHistoryRepository.findAll().stream()
				.filter(history -> history.getUserId().equals(userId))
				.collect(Collectors.toList());
		long chargeHistoryCount = histories.stream()
				.filter(history -> history.getType() == PointHistoryType.CHARGE)
				.count();
		long useHistoryCount = histories.stream()
				.filter(history -> history.getType() == PointHistoryType.USE)
				.count();
		assertThat(chargeHistoryCount).isEqualTo(runResult.chargeSuccessCount);
		assertThat(useHistoryCount).isEqualTo(runResult.orderSuccessCount);
		assertThat(histories).hasSize(runResult.chargeSuccessCount + runResult.orderSuccessCount);

		// 성공 기준 4: 저장된 orders/order_items 건수 = 성공한 결제 수.
		List<Order> savedOrders = orderRepository.findAll().stream()
				.filter(order -> order.getUserId().equals(userId))
				.collect(Collectors.toList());
		assertThat(savedOrders).hasSize(runResult.orderSuccessCount);

		Set<Long> savedOrderIds = savedOrders.stream().map(Order::getId).collect(Collectors.toSet());
		List<OrderItem> savedOrderItems = orderItemRepository.findAll().stream()
				.filter(item -> savedOrderIds.contains(item.getOrderId()))
				.collect(Collectors.toList());
		assertThat(savedOrderItems).hasSize(runResult.orderSuccessCount);
	}

	/**
	 * 충전 스레드 {@link #CHARGE_THREAD_COUNT}개와 결제 스레드 {@link #ORDER_THREAD_COUNT}개를
	 * {@link CountDownLatch}로 동시 출발시켜 뒤섞어 호출하고, 성공/실패를 집계한다. 두 연산 모두
	 * {@code point:{userId}} 락을 공유하므로 실행 순서는 비결정적이며, 그 순서에 의존하지 않는
	 * 집계값(성공 건수)만 반환한다. {@code INSUFFICIENT_POINT} 외의 예외(충전/결제 어느 쪽이든)는
	 * {@code otherFailures}에 담아 테스트가 명시적으로 비어 있음을 검증하게 한다.
	 */
	private MixedRunResult runConcurrentChargeAndOrder(Long targetUserId, long price) throws Exception {
		int totalThreadCount = CHARGE_THREAD_COUNT + ORDER_THREAD_COUNT;
		CountDownLatch readyLatch = new CountDownLatch(totalThreadCount);
		CountDownLatch startLatch = new CountDownLatch(1);
		ExecutorService executor = Executors.newFixedThreadPool(totalThreadCount);
		AtomicInteger chargeSuccessCount = new AtomicInteger();
		AtomicInteger orderSuccessCount = new AtomicInteger();
		AtomicInteger insufficientPointFailureCount = new AtomicInteger();
		List<Throwable> otherFailures = Collections.synchronizedList(new ArrayList<>());

		try {
			List<Callable<Void>> chargeTasks = IntStream.range(0, CHARGE_THREAD_COUNT)
					.mapToObj(i -> (Callable<Void>) () -> {
						readyLatch.countDown();
						startLatch.await();
						pointService.charge(targetUserId, CHARGE_AMOUNT);
						chargeSuccessCount.incrementAndGet();
						return null;
					})
					.collect(Collectors.toList());

			List<Callable<Void>> orderTasks = IntStream.range(0, ORDER_THREAD_COUNT)
					.mapToObj(i -> (Callable<Void>) () -> {
						readyLatch.countDown();
						startLatch.await();
						orderService.order(new OrderCreateRequest(targetUserId, SEED_MENU_ID, 1));
						orderSuccessCount.incrementAndGet();
						return null;
					})
					.collect(Collectors.toList());

			List<Callable<Void>> allTasks = Stream.concat(chargeTasks.stream(), orderTasks.stream())
					.collect(Collectors.toList());

			List<Future<Void>> futures = allTasks.stream()
					.map(executor::submit)
					.collect(Collectors.toList());

			assertThat(readyLatch.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
			startLatch.countDown();

			for (Future<Void> future : futures) {
				try {
					future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

		return new MixedRunResult(chargeSuccessCount.get(), orderSuccessCount.get(),
				insufficientPointFailureCount.get(), otherFailures);
	}

	/** {@link #runConcurrentChargeAndOrder}의 집계 결과(충전/결제 성공 건수, INSUFFICIENT_POINT 실패 건수, 그 외 실패)를 담는 값 객체. */
	private static class MixedRunResult {

		private final int chargeSuccessCount;
		private final int orderSuccessCount;
		private final int insufficientPointFailureCount;
		private final List<Throwable> otherFailures;

		private MixedRunResult(int chargeSuccessCount, int orderSuccessCount, int insufficientPointFailureCount,
				List<Throwable> otherFailures) {
			this.chargeSuccessCount = chargeSuccessCount;
			this.orderSuccessCount = orderSuccessCount;
			this.insufficientPointFailureCount = insufficientPointFailureCount;
			this.otherFailures = otherFailures;
		}
	}
}
