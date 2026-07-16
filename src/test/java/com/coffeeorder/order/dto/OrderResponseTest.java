package com.coffeeorder.order.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.entity.OrderStatus;
import com.coffeeorder.order.service.OrderResult;

class OrderResponseTest {

	@Test
	void from_주문결과를_응답_DTO로_매핑한다() {
		Order order = new Order(1L, 3500L, OrderStatus.PAID, LocalDateTime.now());
		OrderItem orderItem = new OrderItem(order.getId(), 2L, "카페라떼", 3500L, 1);
		OrderResult result = new OrderResult(order, List.of(orderItem), 21500L);

		OrderResponse response = OrderResponse.from(result);

		assertThat(response.getOrderId()).isEqualTo(order.getId());
		assertThat(response.getUserId()).isEqualTo(1L);
		assertThat(response.getTotalAmount()).isEqualTo(3500L);
		assertThat(response.getBalanceAfter()).isEqualTo(21500L);
		assertThat(response.getStatus()).isEqualTo(OrderStatus.PAID);
		assertThat(response.getStatus().name()).isEqualTo("PAID");

		assertThat(response.getItems()).hasSize(1);
		OrderItemResponse itemResponse = response.getItems().get(0);
		assertThat(itemResponse.getMenuId()).isEqualTo(2L);
		assertThat(itemResponse.getName()).isEqualTo("카페라떼");
		assertThat(itemResponse.getUnitPrice()).isEqualTo(3500L);
		assertThat(itemResponse.getQuantity()).isEqualTo(1);
	}
}
