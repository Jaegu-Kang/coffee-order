package com.coffeeorder.order.service;

import java.util.List;

import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;

/**
 * {@link OrderService#order}의 처리 결과. 컨트롤러/응답 DTO 티켓(SCRUM-55 범위 밖)이 아직
 * 없으므로, 이후 티켓이 응답을 조립할 수 있도록 저장된 {@link Order}, {@link OrderItem} 목록,
 * 차감 후 잔액({@code balanceAfter})을 최소한으로 담아 반환한다(docs/api/order.md 응답 예시의
 * {@code balanceAfter} 참고).
 */
public class OrderResult {

	private final Order order;
	private final List<OrderItem> orderItems;
	private final Long balanceAfter;

	public OrderResult(Order order, List<OrderItem> orderItems, Long balanceAfter) {
		this.order = order;
		this.orderItems = orderItems;
		this.balanceAfter = balanceAfter;
	}

	public Order getOrder() {
		return order;
	}

	public List<OrderItem> getOrderItems() {
		return orderItems;
	}

	public Long getBalanceAfter() {
		return balanceAfter;
	}
}
