package com.coffeeorder.common.dto;

/**
 * 공통 에러 응답 DTO.
 * 형식: { "code": "...", "message": "..." }
 */
public class ErrorResponse {

	private final String code;
	private final String message;

	public ErrorResponse(String code, String message) {
		this.code = code;
		this.message = message;
	}

	public static ErrorResponse of(String code, String message) {
		return new ErrorResponse(code, message);
	}

	public String getCode() {
		return code;
	}

	public String getMessage() {
		return message;
	}
}
