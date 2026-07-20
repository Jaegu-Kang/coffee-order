package com.coffeeorder.common.lock;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;

/**
 * 테스트 전용 {@link RedisDistributedLock} 대역(더블, SCRUM-74 / E6-2 태스크5).
 * 샌드박스/CI에 실제 Redis가 기동돼 있지 않아, {@code PointService}/{@code OrderService}의
 * N-스레드 동시성 테스트가 외부 Redis 가용성에 의존하면 {@code ./gradlew test}가 불안정해진다.
 * 이를 피하기 위해 {@code executeWithLock}만 재정의해 실제 계약("키별 상호배제 + waitTime 내에
 * 락을 얻지 못하면 {@link ErrorCode#CONCURRENCY_CONFLICT}")을
 * {@link ConcurrentHashMap}과 {@link ReentrantLock#tryLock(long, TimeUnit)}으로 동일하게
 * 재현한다.
 * <p>
 * 이 대역은 단일 JVM 프로세스 내 상호배제만 재현하므로, "다중 인스턴스 간 실제 Redis 분산락"
 * 자체의 정합성 증명은 이 클래스의 책임이 아니다(그 부분은 {@link RedisDistributedLockTest}가
 * 단위 수준으로 이미 커버하며, 추후 E6-4-3 Testcontainers 통합 테스트가 실제 broker 수준으로
 * 커버할 예정이다). 프로덕션 {@link RedisDistributedLock}은 이 클래스에서 수정하지 않는다.
 * 상위 클래스 생성자가 요구하는 {@code RedisTemplate}은 {@link #executeWithLock}을 완전히
 * 재정의해 상위 클래스의 private 메서드를 전혀 사용하지 않으므로 {@code null}로 전달한다.
 */
public class InMemoryRedisDistributedLock extends RedisDistributedLock {

	private final ConcurrentHashMap<String, ReentrantLock> locksByKey = new ConcurrentHashMap<>();

	public InMemoryRedisDistributedLock() {
		super(null);
	}

	@Override
	public <T> T executeWithLock(String key, Duration waitTime, Duration leaseTime, Supplier<T> task) {
		ReentrantLock lock = locksByKey.computeIfAbsent(key, unusedKey -> new ReentrantLock());

		boolean acquired;
		try {
			acquired = lock.tryLock(waitTime.toMillis(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new BusinessException(ErrorCode.CONCURRENCY_CONFLICT);
		}

		if (!acquired) {
			throw new BusinessException(ErrorCode.CONCURRENCY_CONFLICT);
		}

		try {
			return task.get();
		} finally {
			lock.unlock();
		}
	}
}
