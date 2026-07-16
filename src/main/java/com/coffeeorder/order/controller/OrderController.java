package com.coffeeorder.order.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;

import com.coffeeorder.order.dto.OrderCreateRequest;
import com.coffeeorder.order.dto.OrderResponse;
import com.coffeeorder.order.service.OrderService;

/**
 * 주문/결제 API. {@code docs/api/order.md}의 {@code POST /api/orders}를 제공한다.
 * 검증·존재확인·포인트 차감 등 처리는 모두 {@link OrderService}에 위임하고,
 * 예외는 {@code GlobalExceptionHandler}가 공통 처리하므로 이 컨트롤러는 얇게 유지한다.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

	private final OrderService orderService;

	public OrderController(OrderService orderService) {
		this.orderService = orderService;
	}

	@PostMapping
	public ResponseEntity<OrderResponse> order(@Valid @RequestBody OrderCreateRequest request) {
		OrderResponse response = OrderResponse.from(orderService.order(request));
		return ResponseEntity.status(HttpStatus.CREATED).body(response);
	}
}
