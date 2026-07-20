package com.coffeeorder.order.service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.common.lock.RedisDistributedLock;
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
 * 1~2, 4~5단계(존재확인 → 총액 계산 → 포인트 차감/이력 기록 → 주문·주문항목 저장)를 하나의
 * 트랜잭션으로 처리한다(SCRUM-55, SCRUM-61).
 * <p>
 * 3단계(잔액 조회~차감~{@code USE} 이력 기록)는 {@code point:{userId}} 키의 Redis
 * 분산락({@link RedisDistributedLock})으로 다중 인스턴스 간 직렬화하고, 그 안에서
 * {@link PointBalanceRepository#findByIdForUpdate(Long)}로 비관적 락 조회를 수행해 동일
 * 인스턴스 내 동시 요청도 직렬화한다(SCRUM-73 / E6-2 태스크4).
 * <p>
 * 6단계({@code order-events} 토픽 발행)는 SCRUM-76(E6-3 태스크2)에서 커밋 이전 동기
 * {@code kafkaTemplate.send(...)} 호출을 제거하고, {@link ApplicationEventPublisher#publishEvent}로
 * {@link OrderEvent}를 애플리케이션 이벤트로만 발행하도록 전환했다. 실제 Kafka 발행은 트랜잭션 커밋
 * 이후({@code AFTER_COMMIT})에만 {@link com.coffeeorder.order.event.OrderEventKafkaListener}가
 * 담당하므로, {@code docs/api/order.md} 6번("커밋 후 발행")과 실제 구현이 일치한다. 발행-트랜잭션
 * 정합성 강화 중 롤백 시 미발행 테스트(태스크3)·Outbox(태스크4)는 별도 후속 범위다
 * (docs/design/kafka-after-commit-check.md).
 * <p>
 * 재점검 결과(E6-3 태스크1, docs/design/jira-manual.md 246행): {@code order()}는 클래스 레벨
 * {@code @Transactional(readOnly = true)}를 오버라이드하는 단일 {@code @Transactional} 메서드이고,
 * {@link RedisDistributedLock#executeWithLock}은 별도 스레드/트랜잭션을 열지 않고 호출 스레드의
 * 트랜잭션 안에서 동기 실행되며, 포인트 차감~주문/주문항목 저장 사이에 {@code REQUIRES_NEW} 등
 * 트랜잭션 경계를 끊는 코드나 self-invocation이 없다. 따라서 포인트 차감(잔액 갱신 + {@code USE}
 * 이력 insert)부터 {@code orders}/{@code order_items} 저장까지가 물리적으로 하나의 트랜잭션이며,
 * 어느 단계든 실패 시 전체 롤백됨을
 * {@link com.coffeeorder.order.service.OrderServiceAtomicityTest}에서 실증했다(기존 사용자의
 * 잔액 UPDATE 롤백, 신규 사용자의 잔액 행 INSERT 자체 롤백 두 케이스 모두 포함).
 */
@Service
@Transactional(readOnly = true)
public class OrderService {

	private static final int DEFAULT_QUANTITY = 1;
	private static final String POINT_LOCK_KEY_PREFIX = "point:";
	private static final Duration LOCK_WAIT_TIME = Duration.ofSeconds(3);
	private static final Duration LOCK_LEASE_TIME = Duration.ofSeconds(5);

	private final OrderRepository orderRepository;
	private final OrderItemRepository orderItemRepository;
	private final UserRepository userRepository;
	private final MenuRepository menuRepository;
	private final PointBalanceRepository pointBalanceRepository;
	private final PointHistoryRepository pointHistoryRepository;
	private final ApplicationEventPublisher eventPublisher;
	private final RedisDistributedLock redisDistributedLock;

	public OrderService(OrderRepository orderRepository,
			OrderItemRepository orderItemRepository,
			UserRepository userRepository,
			MenuRepository menuRepository,
			PointBalanceRepository pointBalanceRepository,
			PointHistoryRepository pointHistoryRepository,
			ApplicationEventPublisher eventPublisher,
			RedisDistributedLock redisDistributedLock) {
		this.orderRepository = orderRepository;
		this.orderItemRepository = orderItemRepository;
		this.userRepository = userRepository;
		this.menuRepository = menuRepository;
		this.pointBalanceRepository = pointBalanceRepository;
		this.pointHistoryRepository = pointHistoryRepository;
		this.eventPublisher = eventPublisher;
		this.redisDistributedLock = redisDistributedLock;
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

		Long balanceAfter = redisDistributedLock.executeWithLock(
				POINT_LOCK_KEY_PREFIX + userId, LOCK_WAIT_TIME, LOCK_LEASE_TIME, () -> {
					PointBalance pointBalance = pointBalanceRepository.findByIdForUpdate(userId)
							.orElseGet(() -> new PointBalance(userId, 0L));
					if (pointBalance.getBalance() < totalAmount) {
						throw new BusinessException(ErrorCode.INSUFFICIENT_POINT);
					}

					pointBalance.deduct(totalAmount);
					pointBalanceRepository.save(pointBalance);
					pointHistoryRepository.save(
							new PointHistory(userId, PointHistory.TYPE_USE, totalAmount, pointBalance.getBalance()));

					return pointBalance.getBalance();
				});

		LocalDateTime orderedAt = LocalDateTime.now(ZoneOffset.UTC);
		Order order = orderRepository.save(new Order(userId, totalAmount, OrderStatus.PAID, orderedAt));

		OrderItem orderItem = orderItemRepository.save(
				new OrderItem(order.getId(), menu.getId(), menu.getName(), menu.getPrice(), quantity));

		eventPublisher.publishEvent(OrderEvent.from(order, orderItem));

		return new OrderResult(order, List.of(orderItem), balanceAfter);
	}
}
