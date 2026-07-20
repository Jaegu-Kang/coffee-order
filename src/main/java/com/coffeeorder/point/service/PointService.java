package com.coffeeorder.point.service;

import java.time.Duration;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.common.lock.RedisDistributedLock;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.entity.PointHistoryType;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.repository.UserRepository;

/**
 * 포인트 충전 비즈니스 로직. {@code docs/api/point.md}의 충전 API를 지원한다.
 * 사용자 존재 확인 → 잔액 증가 → {@link PointHistoryType#CHARGE} 이력 기록을 단일 트랜잭션으로
 * 처리한다. 잔액 조회~저장~이력 기록 구간은 {@code point:{userId}} 키의 Redis 분산락({@link RedisDistributedLock})
 * 으로 다중 인스턴스 간 직렬화하고, 그 안에서 {@link PointBalanceRepository#findByIdForUpdate(Long)}로
 * 비관적 락 조회를 수행해 동일 인스턴스 내 동시 요청도 직렬화한다(SCRUM-73 / E6-2 태스크4).
 */
@Service
@Transactional(readOnly = true)
public class PointService {

	private static final String POINT_LOCK_KEY_PREFIX = "point:";
	private static final Duration LOCK_WAIT_TIME = Duration.ofSeconds(3);
	private static final Duration LOCK_LEASE_TIME = Duration.ofSeconds(5);

	private final UserRepository userRepository;
	private final PointBalanceRepository pointBalanceRepository;
	private final PointHistoryRepository pointHistoryRepository;
	private final RedisDistributedLock redisDistributedLock;

	public PointService(
			UserRepository userRepository,
			PointBalanceRepository pointBalanceRepository,
			PointHistoryRepository pointHistoryRepository,
			RedisDistributedLock redisDistributedLock) {
		this.userRepository = userRepository;
		this.pointBalanceRepository = pointBalanceRepository;
		this.pointHistoryRepository = pointHistoryRepository;
		this.redisDistributedLock = redisDistributedLock;
	}

	@Transactional
	public Long charge(Long userId, Long amount) {
		if (amount == null || amount <= 0) {
			throw new BusinessException(ErrorCode.INVALID_AMOUNT);
		}
		if (!userRepository.existsById(userId)) {
			throw new BusinessException(ErrorCode.USER_NOT_FOUND);
		}

		return redisDistributedLock.executeWithLock(
				POINT_LOCK_KEY_PREFIX + userId, LOCK_WAIT_TIME, LOCK_LEASE_TIME, () -> {
					PointBalance pointBalance = pointBalanceRepository.findByIdForUpdate(userId)
							.orElseGet(() -> new PointBalance(userId));
					pointBalance.charge(amount);
					pointBalanceRepository.save(pointBalance);

					PointHistory pointHistory = new PointHistory(
							userId, PointHistoryType.CHARGE, amount, pointBalance.getBalance());
					pointHistoryRepository.save(pointHistory);

					return pointBalance.getBalance();
				});
	}
}
