package com.coffeeorder.point.dto;

/**
 * 포인트 충전 응답 DTO. {@code docs/api/point.md}의
 * {@code POST /api/points/charge} 응답 바디({@code userId}, {@code balance})에 대응한다.
 */
public class PointChargeResponse {

	private final Long userId;
	private final Long balance;

	public PointChargeResponse(Long userId, Long balance) {
		this.userId = userId;
		this.balance = balance;
	}

	public static PointChargeResponse from(Long userId, Long balance) {
		return new PointChargeResponse(userId, balance);
	}

	public Long getUserId() {
		return userId;
	}

	public Long getBalance() {
		return balance;
	}
}
