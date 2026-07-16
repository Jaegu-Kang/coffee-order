package com.coffeeorder.order.dto;

import com.coffeeorder.order.entity.OrderItem;

/**
 * 주문 항목 응답 DTO. {@code docs/api/order.md}의 {@code POST /api/orders} 응답 예시 중
 * {@code items} 배열 원소({@code menuId}, {@code name}, {@code unitPrice}, {@code quantity})에
 * 대응한다. 엔티티({@link OrderItem})를 직접 노출하지 않고 필요한 필드만 매핑하며,
 * 엔티티의 {@code menuName} 필드는 응답 필드명 {@code name}으로 매핑한다.
 */
public class OrderItemResponse {

	private final Long menuId;
	private final String name;
	private final Long unitPrice;
	private final Integer quantity;

	public OrderItemResponse(Long menuId, String name, Long unitPrice, Integer quantity) {
		this.menuId = menuId;
		this.name = name;
		this.unitPrice = unitPrice;
		this.quantity = quantity;
	}

	public static OrderItemResponse from(OrderItem orderItem) {
		return new OrderItemResponse(
			orderItem.getMenuId(),
			orderItem.getMenuName(),
			orderItem.getUnitPrice(),
			orderItem.getQuantity());
	}

	public Long getMenuId() {
		return menuId;
	}

	public String getName() {
		return name;
	}

	public Long getUnitPrice() {
		return unitPrice;
	}

	public Integer getQuantity() {
		return quantity;
	}
}
