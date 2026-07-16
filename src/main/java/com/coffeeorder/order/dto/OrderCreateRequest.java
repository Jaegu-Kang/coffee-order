package com.coffeeorder.order.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * 주문 생성 요청 DTO. {@code docs/api/order.md}의 {@code POST /api/orders} 요청 바디에 대응한다.
 * {@code quantity}의 기본값(1) 적용은 이 DTO의 책임이 아니며, 서비스 계층에서 처리한다.
 */
public class OrderCreateRequest {

	@NotNull
	private final Long userId;

	@NotNull
	private final Long menuId;

	@Positive
	private final Integer quantity;

	public OrderCreateRequest(Long userId, Long menuId, Integer quantity) {
		this.userId = userId;
		this.menuId = menuId;
		this.quantity = quantity;
	}

	public Long getUserId() {
		return userId;
	}

	public Long getMenuId() {
		return menuId;
	}

	public Integer getQuantity() {
		return quantity;
	}
}
