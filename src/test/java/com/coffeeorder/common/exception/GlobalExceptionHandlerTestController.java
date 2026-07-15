package com.coffeeorder.common.exception;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@link GlobalExceptionHandler} 슬라이스 테스트 전용 더미 컨트롤러.
 * 실제 도메인 컨트롤러가 아직 없어 예외 매핑 검증을 위해 사용한다.
 */
@RestController
@RequestMapping("/test/exceptions")
public class GlobalExceptionHandlerTestController {

	@PostMapping("/user-not-found")
	public void triggerUserNotFound() {
		throw new BusinessException(ErrorCode.USER_NOT_FOUND);
	}

	@PostMapping("/insufficient-point")
	public void triggerInsufficientPointWithCustomMessage() {
		throw new BusinessException(ErrorCode.INSUFFICIENT_POINT, "커스텀 메시지");
	}

	@PostMapping("/validate")
	public void triggerValidationError(@Valid @RequestBody TestRequest request) {
	}

	public static class TestRequest {

		@NotNull
		private Long userId;

		public Long getUserId() {
			return userId;
		}

		public void setUserId(Long userId) {
			this.userId = userId;
		}
	}
}
