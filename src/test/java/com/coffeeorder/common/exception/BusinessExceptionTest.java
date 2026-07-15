package com.coffeeorder.common.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BusinessExceptionTest {

	@Test
	void 기본_생성자는_ErrorCode의_defaultMessage를_사용한다() {
		BusinessException exception = new BusinessException(ErrorCode.USER_NOT_FOUND);

		assertThat(exception.getMessage()).isEqualTo(ErrorCode.USER_NOT_FOUND.getDefaultMessage());
	}

	@Test
	void 커스텀_메시지_생성자는_전달받은_메시지를_사용한다() {
		String customMessage = "커스텀 메시지";

		BusinessException exception = new BusinessException(ErrorCode.INSUFFICIENT_POINT, customMessage);

		assertThat(exception.getMessage()).isEqualTo(customMessage);
	}

	@Test
	void getErrorCode는_생성_시_전달한_ErrorCode를_반환한다() {
		BusinessException exception = new BusinessException(ErrorCode.INVALID_AMOUNT);

		assertThat(exception.getErrorCode()).isSameAs(ErrorCode.INVALID_AMOUNT);
	}

	@Test
	void BusinessException은_RuntimeException의_인스턴스이다() {
		BusinessException exception = new BusinessException(ErrorCode.CONCURRENCY_CONFLICT);

		assertThat(exception).isInstanceOf(RuntimeException.class);
	}
}
