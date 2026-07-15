package com.coffeeorder.common.exception;

import org.springframework.http.HttpStatus;

/**
 * 공통 에러코드 상수 정의.
 * `code`는 대문자 SNAKE_CASE이며 enum 상수명과 일치한다(`docs/policy.md` 네임스페이스 규칙).
 * 향후 `BusinessException`/`GlobalExceptionHandler`(SCRUM-31, -32)에서 이 enum을 사용해
 * `{ "code": "...", "message": "..." }` 형식의 에러 응답을 생성할 예정이다.
 */
public enum ErrorCode {

	VALIDATION_ERROR("VALIDATION_ERROR", HttpStatus.BAD_REQUEST, "요청 값이 올바르지 않습니다."),
	USER_NOT_FOUND("USER_NOT_FOUND", HttpStatus.NOT_FOUND, "사용자를 찾을 수 없습니다."),
	MENU_NOT_FOUND("MENU_NOT_FOUND", HttpStatus.NOT_FOUND, "메뉴를 찾을 수 없습니다."),
	INSUFFICIENT_POINT("INSUFFICIENT_POINT", HttpStatus.CONFLICT, "포인트 잔액이 부족합니다."),
	INVALID_AMOUNT("INVALID_AMOUNT", HttpStatus.BAD_REQUEST, "금액은 0보다 커야 합니다."),
	CONCURRENCY_CONFLICT("CONCURRENCY_CONFLICT", HttpStatus.CONFLICT, "동시 요청 처리 중 충돌이 발생했습니다.");

	private final String code;
	private final HttpStatus httpStatus;
	private final String defaultMessage;

	ErrorCode(String code, HttpStatus httpStatus, String defaultMessage) {
		this.code = code;
		this.httpStatus = httpStatus;
		this.defaultMessage = defaultMessage;
	}

	public String getCode() {
		return code;
	}

	public HttpStatus getHttpStatus() {
		return httpStatus;
	}

	public String getDefaultMessage() {
		return defaultMessage;
	}
}
