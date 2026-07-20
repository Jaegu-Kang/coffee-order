package com.coffeeorder.point.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.entity.User;
import com.coffeeorder.user.repository.UserRepository;

/**
 * {@link PointService#charge}의 동시성 테스트(SCRUM-21 AC / SCRUM-74, E6-2 태스크5).
 * <p>
 * 무엇을: 동일 사용자에 대해 {@value #THREAD_COUNT}개 스레드가 {@link CountDownLatch}로 동시에
 * 출발해 같은 금액을 {@code charge()}로 동시 요청한다.
 * <p>
 * 왜: "동일 사용자 동시 충전/결제 N건 → 분실 갱신 없음, 최종 잔액=기대값"이라는 AC를 검증하기
 * 위함이다. {@code point:{userId}} Redis 분산락({@link com.coffeeorder.common.lock.RedisDistributedLock})
 * → {@link PointBalanceRepository#findByIdForUpdate(Long)} 비관적 락 순으로 구현된 실제 서비스
 * 흐름 전체(락+트랜잭션+잔액/이력 갱신)를 검증하는 것이 목적이며, 이는 기존 Mockito 단위
 * 테스트({@link PointServiceTest}, 락을 즉시 통과하도록 스텁)나
 * {@code PointBalanceRepositoryLockTest}(리포지토리 단독, Redis 불필요)로는 커버되지 않던
 * 공백이다.
 * <p>
 * 어떻게: 샌드박스/CI에 실제 Redis가 없어
 * {@link com.coffeeorder.common.lock.InMemoryRedisDistributedLock} 대역(계약은 동일: 키별
 * 상호배제 + waitTime 내 미획득 시 {@code CONCURRENCY_CONFLICT})을 사용한다. 이 대역은 단일
 * JVM 내 상호배제만 재현하므로, 다중 인스턴스 간 실제 Redis 분산락 자체의 검증은 이 테스트의
 * 책임이 아니다(한계, {@code InMemoryRedisDistributedLock} Javadoc 참고). {@code PointService}를
 * {@code @Import}로 등록해 Spring이 관리하는 실제 {@code @Transactional} 프록시를
 * {@code @Autowired}로 사용한다 — 수동 {@code new PointService(...)}는 트랜잭션 프록시가
 * 없어 비관적 락이 조기 해제되므로 반드시 회피해야 한다.
 * <p>
 * {@code PointBalanceRepositoryLockTest}와 동일하게, 스레드별로 별도 커밋 트랜잭션이 필요하므로
 * {@code @DataJpaTest}의 테스트-메서드 트랜잭션 래핑을
 * {@code @Transactional(propagation = Propagation.NOT_SUPPORTED)}로 끈다. 시드 사용자(user_id
 * 1~3)와 무관한 신규 사용자를 생성해 사용하고, {@link #cleanUp()}에서 생성한 행을 정리한다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({JpaAuditingConfig.class, PointService.class, InMemoryRedisDistributedLockTestConfig.class})
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class PointServiceConcurrencyTest {

	private static final int THREAD_COUNT = 20;
	private static final long CHARGE_AMOUNT = 1000L;
	private static final long AWAIT_TIMEOUT_SECONDS = 30L;

	@Autowired
	private PointService pointService;

	@Autowired
	private UserRepository userRepository;

	@Autowired
	private PointBalanceRepository pointBalanceRepository;

	@Autowired
	private PointHistoryRepository pointHistoryRepository;

	@Autowired
	private PlatformTransactionManager transactionManager;

	private Long userId;

	@AfterEach
	void cleanUp() {
		if (userId == null) {
			return;
		}
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		Long cleanupUserId = userId;
		transactionTemplate.executeWithoutResult(status -> {
			pointHistoryRepository.findAll().stream()
					.filter(history -> history.getUserId().equals(cleanupUserId))
					.forEach(pointHistoryRepository::delete);
			pointBalanceRepository.findById(cleanupUserId).ifPresent(pointBalanceRepository::delete);
			userRepository.findById(cleanupUserId).ifPresent(userRepository::delete);
		});
	}

	@Test
	void 동일_사용자에_대한_N개_동시_충전_요청은_분실_갱신_없이_최종_잔액과_이력에_모두_반영된다() throws Exception {
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		userId = transactionTemplate.execute(status ->
				userRepository.saveAndFlush(new User("동시성-충전-테스트")).getId());

		CountDownLatch readyLatch = new CountDownLatch(THREAD_COUNT);
		CountDownLatch startLatch = new CountDownLatch(1);
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
		try {
			List<Callable<Long>> chargeTasks = LongStream.range(0, THREAD_COUNT)
					.mapToObj(i -> (Callable<Long>) () -> {
						readyLatch.countDown();
						startLatch.await();
						return pointService.charge(userId, CHARGE_AMOUNT);
					})
					.collect(Collectors.toList());

			List<Future<Long>> futures = chargeTasks.stream()
					.map(executor::submit)
					.collect(Collectors.toList());

			assertThat(readyLatch.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
			startLatch.countDown();

			for (Future<Long> future : futures) {
				future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			}
		} finally {
			executor.shutdown();
		}

		Long finalBalance = pointBalanceRepository.findById(userId).orElseThrow().getBalance();
		assertThat(finalBalance).isEqualTo(THREAD_COUNT * CHARGE_AMOUNT);

		List<PointHistory> histories = pointHistoryRepository.findAll().stream()
				.filter(history -> history.getUserId().equals(userId))
				.collect(Collectors.toList());
		assertThat(histories).hasSize(THREAD_COUNT);

		List<Long> expectedBalanceAfters = LongStream.rangeClosed(1, THREAD_COUNT)
				.map(step -> step * CHARGE_AMOUNT)
				.boxed()
				.sorted()
				.collect(Collectors.toList());
		List<Long> actualBalanceAfters = histories.stream()
				.map(PointHistory::getBalanceAfter)
				.sorted()
				.collect(Collectors.toList());
		assertThat(actualBalanceAfters).isEqualTo(expectedBalanceAfters);
	}
}
