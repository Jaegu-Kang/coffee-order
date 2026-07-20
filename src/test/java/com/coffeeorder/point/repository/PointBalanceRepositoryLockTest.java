package com.coffeeorder.point.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.point.entity.PointBalance;

/**
 * {@link PointBalanceRepository#findByIdForUpdate(Long)}가 실제로 비관적 쓰기 락
 * ({@code SELECT ... FOR UPDATE})을 걸어 동시 접근을 직렬화하는지 검증한다.
 * <p>
 * {@code @DataJpaTest}는 기본적으로 테스트 메서드 전체를 하나의 트랜잭션으로 감싸고 종료 시 롤백하므로,
 * 서로 독립된 두 트랜잭션이 실제로 동시에 실행되는 상황을 재현할 수 없다. 이를 위해 이 테스트 클래스는
 * {@code @Transactional(propagation = Propagation.NOT_SUPPORTED)}로 테스트 메서드 자체의 트랜잭션
 * 래핑을 끄고, {@link TransactionTemplate}으로 두 개의 실제(커밋되는) 트랜잭션을 만들어 검증한다.
 * <p>
 * 각 트랜잭션이 실제로 커밋되므로, 테스트가 남긴 {@code point_balances} 행은 {@link #cleanUp()}에서
 * 매 테스트 후 제거해 다른 테스트(예: {@link PointBalanceRepositoryTest})가 사용하는 시드 사용자
 * (user_id 1~3)의 상태에 영향을 주지 않도록 한다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class PointBalanceRepositoryLockTest {

	private static final long LOCK_HOLD_MILLIS = 500L;
	private static final long AWAIT_TIMEOUT_SECONDS = 10L;

	@Autowired
	private PointBalanceRepository pointBalanceRepository;

	@Autowired
	private PlatformTransactionManager transactionManager;

	@AfterEach
	void cleanUp() {
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		transactionTemplate.executeWithoutResult(status -> {
			pointBalanceRepository.findById(1L).ifPresent(pointBalanceRepository::delete);
			pointBalanceRepository.findById(2L).ifPresent(pointBalanceRepository::delete);
		});
	}

	@Test
	void findByIdForUpdate는_정상_조회_시_findById와_동일한_엔티티를_반환한다() {
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

		transactionTemplate.executeWithoutResult(status ->
				pointBalanceRepository.saveAndFlush(new PointBalance(1L, 500L)));

		PointBalance found = transactionTemplate.execute(status ->
				pointBalanceRepository.findByIdForUpdate(1L).orElseThrow());

		assertThat(found.getUserId()).isEqualTo(1L);
		assertThat(found.getBalance()).isEqualTo(500L);
	}

	@Test
	void 두_트랜잭션이_동시에_같은_user_id를_락_조회하면_두_번째는_첫_번째_트랜잭션_종료까지_블로킹된다() throws Exception {
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

		transactionTemplate.executeWithoutResult(status ->
				pointBalanceRepository.saveAndFlush(new PointBalance(2L, 1000L)));

		CountDownLatch firstLockAcquired = new CountDownLatch(1);
		AtomicLong firstTransactionEndedAt = new AtomicLong();
		AtomicLong secondLockAcquiredAt = new AtomicLong();

		ExecutorService executor = Executors.newFixedThreadPool(2);
		try {
			Future<?> firstTransaction = executor.submit(() -> transactionTemplate.executeWithoutResult(status -> {
				pointBalanceRepository.findByIdForUpdate(2L).orElseThrow();
				firstLockAcquired.countDown();
				try {
					Thread.sleep(LOCK_HOLD_MILLIS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				firstTransactionEndedAt.set(System.nanoTime());
			}));

			assertThat(firstLockAcquired.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

			Future<?> secondTransaction = executor.submit(() -> transactionTemplate.executeWithoutResult(status -> {
				pointBalanceRepository.findByIdForUpdate(2L).orElseThrow();
				secondLockAcquiredAt.set(System.nanoTime());
			}));

			secondTransaction.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			firstTransaction.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		} finally {
			executor.shutdown();
		}

		assertThat(secondLockAcquiredAt.get()).isGreaterThanOrEqualTo(firstTransactionEndedAt.get());
	}
}
