package com.coffeeorder.point.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.entity.PointHistoryType;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.repository.UserRepository;

@ExtendWith(MockitoExtension.class)
class PointServiceTest {

	@Mock
	private UserRepository userRepository;

	@Mock
	private PointBalanceRepository pointBalanceRepository;

	@Mock
	private PointHistoryRepository pointHistoryRepository;

	@Test
	void charge_기존_잔액이_있는_사용자는_정상_충전된다() {
		Long userId = 1L;
		PointBalance pointBalance = new PointBalance(userId);
		pointBalance.charge(1000L);
		when(userRepository.existsById(userId)).thenReturn(true);
		when(pointBalanceRepository.findById(userId)).thenReturn(Optional.of(pointBalance));

		PointService pointService = new PointService(userRepository, pointBalanceRepository, pointHistoryRepository);

		Long result = pointService.charge(userId, 500L);

		assertThat(result).isEqualTo(1500L);
		verify(pointBalanceRepository).save(pointBalance);

		ArgumentCaptor<PointHistory> historyCaptor = ArgumentCaptor.forClass(PointHistory.class);
		verify(pointHistoryRepository).save(historyCaptor.capture());
		PointHistory savedHistory = historyCaptor.getValue();
		assertThat(savedHistory.getType()).isEqualTo(PointHistoryType.CHARGE);
		assertThat(savedHistory.getAmount()).isEqualTo(500L);
		assertThat(savedHistory.getBalanceAfter()).isEqualTo(1500L);
	}

	@Test
	void charge_잔액_행이_없는_신규_사용자는_0부터_충전된다() {
		Long userId = 2L;
		when(userRepository.existsById(userId)).thenReturn(true);
		when(pointBalanceRepository.findById(userId)).thenReturn(Optional.empty());

		PointService pointService = new PointService(userRepository, pointBalanceRepository, pointHistoryRepository);

		Long result = pointService.charge(userId, 3000L);

		assertThat(result).isEqualTo(3000L);

		ArgumentCaptor<PointHistory> historyCaptor = ArgumentCaptor.forClass(PointHistory.class);
		verify(pointHistoryRepository).save(historyCaptor.capture());
		assertThat(historyCaptor.getValue().getBalanceAfter()).isEqualTo(3000L);
	}

	@Test
	void charge_존재하지_않는_사용자는_USER_NOT_FOUND_예외가_발생한다() {
		Long userId = 999L;
		when(userRepository.existsById(userId)).thenReturn(false);

		PointService pointService = new PointService(userRepository, pointBalanceRepository, pointHistoryRepository);

		assertThatThrownBy(() -> pointService.charge(userId, 1000L))
				.isInstanceOf(BusinessException.class)
				.satisfies(e -> assertThat(((BusinessException) e).getErrorCode()).isEqualTo(ErrorCode.USER_NOT_FOUND));

		verifyNoInteractions(pointBalanceRepository, pointHistoryRepository);
	}

	@Test
	void charge_amount가_0이면_INVALID_AMOUNT_예외가_발생한다() {
		PointService pointService = new PointService(userRepository, pointBalanceRepository, pointHistoryRepository);

		assertThatThrownBy(() -> pointService.charge(1L, 0L))
				.isInstanceOf(BusinessException.class)
				.satisfies(e -> assertThat(((BusinessException) e).getErrorCode()).isEqualTo(ErrorCode.INVALID_AMOUNT));

		verifyNoInteractions(userRepository, pointBalanceRepository, pointHistoryRepository);
	}

	@Test
	void charge_amount가_음수이면_INVALID_AMOUNT_예외가_발생한다() {
		PointService pointService = new PointService(userRepository, pointBalanceRepository, pointHistoryRepository);

		assertThatThrownBy(() -> pointService.charge(1L, -100L))
				.isInstanceOf(BusinessException.class)
				.satisfies(e -> assertThat(((BusinessException) e).getErrorCode()).isEqualTo(ErrorCode.INVALID_AMOUNT));

		verifyNoInteractions(userRepository, pointBalanceRepository, pointHistoryRepository);
	}
}
