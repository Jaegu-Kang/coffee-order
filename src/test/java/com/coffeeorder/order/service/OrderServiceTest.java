package com.coffeeorder.order.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.config.KafkaProducerConfig;
import com.coffeeorder.menu.entity.Menu;
import com.coffeeorder.menu.repository.MenuRepository;
import com.coffeeorder.order.dto.OrderCreateRequest;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.entity.OrderStatus;
import com.coffeeorder.order.event.OrderEvent;
import com.coffeeorder.order.repository.OrderItemRepository;
import com.coffeeorder.order.repository.OrderRepository;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.repository.UserRepository;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

	@Mock
	private OrderRepository orderRepository;

	@Mock
	private OrderItemRepository orderItemRepository;

	@Mock
	private UserRepository userRepository;

	@Mock
	private MenuRepository menuRepository;

	@Mock
	private PointBalanceRepository pointBalanceRepository;

	@Mock
	private PointHistoryRepository pointHistoryRepository;

	@Mock
	private KafkaTemplate<String, Object> kafkaTemplate;

	private OrderService orderService() {
		return new OrderService(orderRepository, orderItemRepository, userRepository, menuRepository,
				pointBalanceRepository, pointHistoryRepository, kafkaTemplate);
	}

	private Menu menu(Long id, String name, Long price) {
		Menu menu = new Menu(name, price);
		ReflectionTestUtils.setField(menu, "id", id);
		return menu;
	}

	@Test
	void order_정상_주문이면_총액을_차감하고_주문_주문항목을_저장한다() {
		Long userId = 1L;
		Menu latte = menu(2L, "카페라떼", 3500L);
		PointBalance balance = new PointBalance(userId, 10000L);
		when(userRepository.existsById(userId)).thenReturn(true);
		when(menuRepository.findById(2L)).thenReturn(Optional.of(latte));
		when(pointBalanceRepository.findById(userId)).thenReturn(Optional.of(balance));
		when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
			Order order = invocation.getArgument(0);
			ReflectionTestUtils.setField(order, "id", 1001L);
			return order;
		});
		when(orderItemRepository.save(any(OrderItem.class))).thenAnswer(invocation -> invocation.getArgument(0));

		OrderCreateRequest request = new OrderCreateRequest(userId, 2L, 2);

		OrderResult result = orderService().order(request);

		assertThat(result.getOrder().getUserId()).isEqualTo(userId);
		assertThat(result.getOrder().getTotalAmount()).isEqualTo(7000L);
		assertThat(result.getOrder().getStatus()).isEqualTo(OrderStatus.PAID);
		assertThat(result.getBalanceAfter()).isEqualTo(3000L);
		assertThat(result.getOrderItems()).hasSize(1);
		assertThat(result.getOrderItems().get(0).getMenuName()).isEqualTo("카페라떼");
		assertThat(result.getOrderItems().get(0).getUnitPrice()).isEqualTo(3500L);
		assertThat(result.getOrderItems().get(0).getQuantity()).isEqualTo(2);

		ArgumentCaptor<PointHistory> historyCaptor = ArgumentCaptor.forClass(PointHistory.class);
		verify(pointHistoryRepository).save(historyCaptor.capture());
		PointHistory savedHistory = historyCaptor.getValue();
		assertThat(savedHistory.getType()).isEqualTo(PointHistory.TYPE_USE);
		assertThat(savedHistory.getAmount()).isEqualTo(7000L);
		assertThat(savedHistory.getBalanceAfter()).isEqualTo(3000L);

		ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<OrderEvent> eventCaptor = ArgumentCaptor.forClass(OrderEvent.class);
		verify(kafkaTemplate).send(topicCaptor.capture(), eventCaptor.capture());
		assertThat(topicCaptor.getValue()).isEqualTo(KafkaProducerConfig.ORDER_EVENTS_TOPIC);
		OrderEvent orderEvent = eventCaptor.getValue();
		assertThat(orderEvent.getOrderId()).isEqualTo(result.getOrder().getId());
		assertThat(orderEvent.getUserId()).isEqualTo(result.getOrder().getUserId());
		assertThat(orderEvent.getMenuId()).isEqualTo(result.getOrderItems().get(0).getMenuId());
		assertThat(orderEvent.getAmount()).isEqualTo(result.getOrder().getTotalAmount());
		assertThat(orderEvent.getOrderedAt())
				.isEqualTo(result.getOrder().getOrderedAt().toInstant(ZoneOffset.UTC));
	}

	@Test
	void order_quantity가_null이면_기본값_1을_적용한다() {
		Long userId = 1L;
		Menu latte = menu(2L, "카페라떼", 3500L);
		PointBalance balance = new PointBalance(userId, 10000L);
		when(userRepository.existsById(userId)).thenReturn(true);
		when(menuRepository.findById(2L)).thenReturn(Optional.of(latte));
		when(pointBalanceRepository.findById(userId)).thenReturn(Optional.of(balance));
		when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> invocation.getArgument(0));
		when(orderItemRepository.save(any(OrderItem.class))).thenAnswer(invocation -> invocation.getArgument(0));

		OrderCreateRequest request = new OrderCreateRequest(userId, 2L, null);

		OrderResult result = orderService().order(request);

		assertThat(result.getOrder().getTotalAmount()).isEqualTo(3500L);
		assertThat(result.getOrderItems().get(0).getQuantity()).isEqualTo(1);

		ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<OrderEvent> eventCaptor = ArgumentCaptor.forClass(OrderEvent.class);
		verify(kafkaTemplate).send(topicCaptor.capture(), eventCaptor.capture());
		assertThat(topicCaptor.getValue()).isEqualTo(KafkaProducerConfig.ORDER_EVENTS_TOPIC);
		OrderEvent orderEvent = eventCaptor.getValue();
		assertThat(orderEvent.getUserId()).isEqualTo(result.getOrder().getUserId());
		assertThat(orderEvent.getMenuId()).isEqualTo(result.getOrderItems().get(0).getMenuId());
		assertThat(orderEvent.getAmount()).isEqualTo(result.getOrder().getTotalAmount());
		assertThat(orderEvent.getOrderedAt())
				.isEqualTo(result.getOrder().getOrderedAt().toInstant(ZoneOffset.UTC));
	}

	@Test
	void order_사용자가_없으면_USER_NOT_FOUND_예외가_발생한다() {
		when(userRepository.existsById(99L)).thenReturn(false);

		OrderCreateRequest request = new OrderCreateRequest(99L, 2L, 1);

		assertThatThrownBy(() -> orderService().order(request))
				.isInstanceOf(BusinessException.class)
				.extracting(exception -> ((BusinessException) exception).getErrorCode())
				.isEqualTo(ErrorCode.USER_NOT_FOUND);

		verify(menuRepository, never()).findById(any());
		verify(orderRepository, never()).save(any());
		verify(kafkaTemplate, never()).send(any(), any());
	}

	@Test
	void order_메뉴가_없으면_MENU_NOT_FOUND_예외가_발생한다() {
		when(userRepository.existsById(1L)).thenReturn(true);
		when(menuRepository.findById(999L)).thenReturn(Optional.empty());

		OrderCreateRequest request = new OrderCreateRequest(1L, 999L, 1);

		assertThatThrownBy(() -> orderService().order(request))
				.isInstanceOf(BusinessException.class)
				.extracting(exception -> ((BusinessException) exception).getErrorCode())
				.isEqualTo(ErrorCode.MENU_NOT_FOUND);

		verify(orderRepository, never()).save(any());
		verify(kafkaTemplate, never()).send(any(), any());
	}

	@Test
	void order_잔액이_부족하면_INSUFFICIENT_POINT_예외가_발생한다() {
		Long userId = 1L;
		Menu latte = menu(2L, "카페라떼", 3500L);
		PointBalance balance = new PointBalance(userId, 1000L);
		when(userRepository.existsById(userId)).thenReturn(true);
		when(menuRepository.findById(2L)).thenReturn(Optional.of(latte));
		when(pointBalanceRepository.findById(userId)).thenReturn(Optional.of(balance));

		OrderCreateRequest request = new OrderCreateRequest(userId, 2L, 1);

		assertThatThrownBy(() -> orderService().order(request))
				.isInstanceOf(BusinessException.class)
				.extracting(exception -> ((BusinessException) exception).getErrorCode())
				.isEqualTo(ErrorCode.INSUFFICIENT_POINT);

		verify(orderRepository, never()).save(any());
		verify(pointHistoryRepository, never()).save(any());
		verify(kafkaTemplate, never()).send(any(), any());
	}

	@Test
	void order_잔액_행이_없으면_0으로_간주해_INSUFFICIENT_POINT_예외가_발생한다() {
		Long userId = 1L;
		Menu latte = menu(2L, "카페라떼", 3500L);
		when(userRepository.existsById(userId)).thenReturn(true);
		when(menuRepository.findById(2L)).thenReturn(Optional.of(latte));
		when(pointBalanceRepository.findById(userId)).thenReturn(Optional.empty());

		OrderCreateRequest request = new OrderCreateRequest(userId, 2L, 1);

		assertThatThrownBy(() -> orderService().order(request))
				.isInstanceOf(BusinessException.class)
				.extracting(exception -> ((BusinessException) exception).getErrorCode())
				.isEqualTo(ErrorCode.INSUFFICIENT_POINT);
	}
}
