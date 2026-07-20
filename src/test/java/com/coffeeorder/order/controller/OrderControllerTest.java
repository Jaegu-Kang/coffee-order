package com.coffeeorder.order.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.entity.OrderStatus;
import com.coffeeorder.order.service.OrderResult;
import com.coffeeorder.order.service.OrderService;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private OrderService orderService;

	@Test
	void order_정상_요청이면_201과_주문_결과를_응답한다() throws Exception {
		Order order = new Order(1L, 3500L, OrderStatus.PAID, LocalDateTime.now());
		ReflectionTestUtils.setField(order, "id", 1001L);
		OrderItem orderItem = new OrderItem(order.getId(), 2L, "카페라떼", 3500L, 1);
		OrderResult result = new OrderResult(order, List.of(orderItem), 21500L);
		when(orderService.order(any())).thenReturn(result);

		mockMvc.perform(post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"userId\":1,\"menuId\":2,\"quantity\":1}"))
				.andExpect(status().isCreated())
				.andExpect(jsonPath("$.orderId").value(1001))
				.andExpect(jsonPath("$.userId").value(1))
				.andExpect(jsonPath("$.items[0].menuId").value(2))
				.andExpect(jsonPath("$.items[0].name").value("카페라떼"))
				.andExpect(jsonPath("$.items[0].unitPrice").value(3500))
				.andExpect(jsonPath("$.items[0].quantity").value(1))
				.andExpect(jsonPath("$.totalAmount").value(3500))
				.andExpect(jsonPath("$.balanceAfter").value(21500))
				.andExpect(jsonPath("$.status").value("PAID"));
	}

	@Test
	void order_userId가_없으면_서비스_호출없이_400_VALIDATION_ERROR를_응답한다() throws Exception {
		mockMvc.perform(post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"menuId\":2,\"quantity\":1}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
	}

	@Test
	void order_menuId가_없으면_서비스_호출없이_400_VALIDATION_ERROR를_응답한다() throws Exception {
		mockMvc.perform(post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"userId\":1,\"quantity\":1}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
	}

	@Test
	void order_quantity가_0이거나_음수이면_서비스_호출없이_400_VALIDATION_ERROR를_응답한다() throws Exception {
		mockMvc.perform(post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"userId\":1,\"menuId\":2,\"quantity\":0}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));

		mockMvc.perform(post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"userId\":1,\"menuId\":2,\"quantity\":-1}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
	}

	@Test
	void order_사용자가_없으면_404_USER_NOT_FOUND를_응답한다() throws Exception {
		when(orderService.order(any())).thenThrow(new BusinessException(ErrorCode.USER_NOT_FOUND));

		mockMvc.perform(post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"userId\":99,\"menuId\":2,\"quantity\":1}"))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.code").value("USER_NOT_FOUND"));
	}

	@Test
	void order_메뉴가_없으면_404_MENU_NOT_FOUND를_응답한다() throws Exception {
		when(orderService.order(any())).thenThrow(new BusinessException(ErrorCode.MENU_NOT_FOUND));

		mockMvc.perform(post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"userId\":1,\"menuId\":999,\"quantity\":1}"))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.code").value("MENU_NOT_FOUND"));
	}

	@Test
	void order_잔액이_부족하면_409_INSUFFICIENT_POINT를_응답한다() throws Exception {
		when(orderService.order(any())).thenThrow(new BusinessException(ErrorCode.INSUFFICIENT_POINT));

		mockMvc.perform(post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"userId\":1,\"menuId\":2,\"quantity\":1}"))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("INSUFFICIENT_POINT"));
	}

	@Test
	void order_동시성_충돌이_발생하면_409_CONCURRENCY_CONFLICT를_응답한다() throws Exception {
		when(orderService.order(any())).thenThrow(new BusinessException(ErrorCode.CONCURRENCY_CONFLICT));

		mockMvc.perform(post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"userId\":1,\"menuId\":2,\"quantity\":1}"))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.code").value("CONCURRENCY_CONFLICT"));
	}
}
