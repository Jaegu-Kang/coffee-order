package com.coffeeorder.point.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.entity.PointHistoryType;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.repository.UserRepository;

/**
 * 포인트 충전 비즈니스 로직. {@code docs/api/point.md}의 충전 API를 지원한다.
 * 사용자 존재 확인 → 잔액 증가 → {@link PointHistoryType#CHARGE} 이력 기록을 단일 트랜잭션으로
 * 처리한다. 비관적 락/Redis 분산락 등 동시성 제어는 이 서비스의 범위가 아니다(E6 도전 과제).
 */
@Service
@Transactional(readOnly = true)
public class PointService {

	private final UserRepository userRepository;
	private final PointBalanceRepository pointBalanceRepository;
	private final PointHistoryRepository pointHistoryRepository;

	public PointService(
			UserRepository userRepository,
			PointBalanceRepository pointBalanceRepository,
			PointHistoryRepository pointHistoryRepository) {
		this.userRepository = userRepository;
		this.pointBalanceRepository = pointBalanceRepository;
		this.pointHistoryRepository = pointHistoryRepository;
	}

	@Transactional
	public Long charge(Long userId, Long amount) {
		if (amount == null || amount <= 0) {
			throw new BusinessException(ErrorCode.INVALID_AMOUNT);
		}
		if (!userRepository.existsById(userId)) {
			throw new BusinessException(ErrorCode.USER_NOT_FOUND);
		}

		PointBalance pointBalance = pointBalanceRepository.findById(userId)
				.orElseGet(() -> new PointBalance(userId));
		pointBalance.charge(amount);
		pointBalanceRepository.save(pointBalance);

		PointHistory pointHistory = new PointHistory(
				userId, PointHistoryType.CHARGE, amount, pointBalance.getBalance());
		pointHistoryRepository.save(pointHistory);

		return pointBalance.getBalance();
	}
}
