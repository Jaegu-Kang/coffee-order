package com.coffeeorder.common.exception;

/**
 * 도메인 예외의 베이스 클래스.
 * {@link ErrorCode}를 감싸는 unchecked 예외로, GlobalExceptionHandler(SCRUM-32)에서
 * ErrorCode 기반으로 { code, message, httpStatus } 응답을 생성하는 데 사용된다.
 */
public class BusinessException extends RuntimeException {

	private final ErrorCode errorCode;

	public BusinessException(ErrorCode errorCode) {
		super(errorCode.getDefaultMessage());
		this.errorCode = errorCode;
	}

	public BusinessException(ErrorCode errorCode, String message) {
		super(message);
		this.errorCode = errorCode;
	}

	public ErrorCode getErrorCode() {
		return errorCode;
	}
}
