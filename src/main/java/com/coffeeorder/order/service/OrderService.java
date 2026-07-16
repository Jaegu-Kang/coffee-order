package com.coffeeorder.order.service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

/**
 * 주문/결제 비즈니스 로직. {@code docs/api/order.md}의 {@code POST /api/orders} 중
 * 1~2, 4~6단계(존재확인 → 총액 계산 → 포인트 차감/이력 기록 → 주문·주문항목 저장 →
 * {@code order-events} 토픽 발행)를 하나의 트랜잭션으로 처리한다(SCRUM-55, SCRUM-61).
 * <p>
 * 3단계(잔액 조회 시 비관적 락)·다중 인스턴스 동시 주문을 막는 Redis 분산락·발행-트랜잭션
 * 정합성 강화(AFTER_COMMIT/Outbox)는 도전 과제(E6) 범위이며 이번 티켓에서는 다루지 않는다
 * (docs/design/jira-backlog.md). 즉 이번 발행은 커밋 이전(트랜잭션 내부) 동기 발행이다.
 */
@Service
@Transactional(readOnly = true)
public class OrderService {

	private static final int DEFAULT_QUANTITY = 1;

	private final OrderRepository orderRepository;
	private final OrderItemRepository orderItemRepository;
	private final UserRepository userRepository;
	private final MenuRepository menuRepository;
	private final PointBalanceRepository pointBalanceRepository;
	private final PointHistoryRepository pointHistoryRepository;
	private final KafkaTemplate<String, Object> kafkaTemplate;

	public OrderService(OrderRepository orderRepository,
			OrderItemRepository orderItemRepository,
			UserRepository userRepository,
			MenuRepository menuRepository,
			PointBalanceRepository pointBalanceRepository,
			PointHistoryRepository pointHistoryRepository,
			KafkaTemplate<String, Object> kafkaTemplate) {
		this.orderRepository = orderRepository;
		this.orderItemRepository = orderItemRepository;
		this.userRepository = userRepository;
		this.menuRepository = menuRepository;
		this.pointBalanceRepository = pointBalanceRepository;
		this.pointHistoryRepository = pointHistoryRepository;
		this.kafkaTemplate = kafkaTemplate;
	}

	@Transactional
	public OrderResult order(OrderCreateRequest request) {
		int quantity = request.getQuantity() != null ? request.getQuantity() : DEFAULT_QUANTITY;

		Long userId = request.getUserId();
		if (!userRepository.existsById(userId)) {
			throw new BusinessException(ErrorCode.USER_NOT_FOUND);
		}

		Menu menu = menuRepository.findById(request.getMenuId())
				.orElseThrow(() -> new BusinessException(ErrorCode.MENU_NOT_FOUND));

		long totalAmount = menu.getPrice() * quantity;

		PointBalance pointBalance = pointBalanceRepository.findById(userId)
				.orElseGet(() -> new PointBalance(userId, 0L));
		if (pointBalance.getBalance() < totalAmount) {
			throw new BusinessException(ErrorCode.INSUFFICIENT_POINT);
		}

		pointBalance.deduct(totalAmount);
		pointBalanceRepository.save(pointBalance);
		pointHistoryRepository.save(
				new PointHistory(userId, PointHistory.TYPE_USE, totalAmount, pointBalance.getBalance()));

		LocalDateTime orderedAt = LocalDateTime.now(ZoneOffset.UTC);
		Order order = orderRepository.save(new Order(userId, totalAmount, OrderStatus.PAID, orderedAt));

		OrderItem orderItem = orderItemRepository.save(
				new OrderItem(order.getId(), menu.getId(), menu.getName(), menu.getPrice(), quantity));

		kafkaTemplate.send(KafkaProducerConfig.ORDER_EVENTS_TOPIC, OrderEvent.from(order, orderItem));

		return new OrderResult(order, List.of(orderItem), pointBalance.getBalance());
	}
}
