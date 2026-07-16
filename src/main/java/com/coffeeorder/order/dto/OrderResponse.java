package com.coffeeorder.order.dto;

import java.util.List;
import java.util.stream.Collectors;

import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderStatus;
import com.coffeeorder.order.service.OrderResult;

/**
 * 주문 응답 DTO. {@code docs/api/order.md}의 {@code POST /api/orders} 응답 예시(잔액·상태 포함)에
 * 1:1 대응한다. 엔티티({@link Order}, {@code OrderItem})를 직접 노출하지 않고 {@link OrderResult}로부터
 * 필요한 값만 복사해 매핑한다.
 */
public class OrderResponse {

	private final Long orderId;
	private final Long userId;
	private final List<OrderItemResponse> items;
	private final Long totalAmount;
	private final Long balanceAfter;
	private final OrderStatus status;

	public OrderResponse(
		Long orderId,
		Long userId,
		List<OrderItemResponse> items,
		Long totalAmount,
		Long balanceAfter,
		OrderStatus status) {
		this.orderId = orderId;
		this.userId = userId;
		this.items = items;
		this.totalAmount = totalAmount;
		this.balanceAfter = balanceAfter;
		this.status = status;
	}

	public static OrderResponse from(OrderResult result) {
		Order order = result.getOrder();
		List<OrderItemResponse> items = result.getOrderItems().stream()
			.map(OrderItemResponse::from)
			.collect(Collectors.toList());

		return new OrderResponse(
			order.getId(),
			order.getUserId(),
			items,
			order.getTotalAmount(),
			result.getBalanceAfter(),
			order.getStatus());
	}

	public Long getOrderId() {
		return orderId;
	}

	public Long getUserId() {
		return userId;
	}

	public List<OrderItemResponse> getItems() {
		return items;
	}

	public Long getTotalAmount() {
		return totalAmount;
	}

	public Long getBalanceAfter() {
		return balanceAfter;
	}

	public OrderStatus getStatus() {
		return status;
	}
}
