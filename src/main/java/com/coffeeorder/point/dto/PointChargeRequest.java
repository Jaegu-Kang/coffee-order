package com.coffeeorder.point.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * 포인트 충전 요청 DTO. {@code docs/api/point.md}의
 * {@code POST /api/points/charge} 요청 바디({@code userId}, {@code amount})에 대응한다.
 * 필드 타입은 {@code point_balances}/{@code point_histories}의 컬럼 타입({@code Long})과 일치시킨다.
 */
public class PointChargeRequest {

	@NotNull
	private Long userId;

	@NotNull
	@Positive
	private Long amount;

	/** Jackson 역직렬화용 기본 생성자. */
	public PointChargeRequest() {
	}

	public PointChargeRequest(Long userId, Long amount) {
		this.userId = userId;
		this.amount = amount;
	}

	public Long getUserId() {
		return userId;
	}

	public Long getAmount() {
		return amount;
	}
}
