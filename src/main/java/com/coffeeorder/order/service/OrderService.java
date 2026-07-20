package com.coffeeorder.order.service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.common.lock.RedisDistributedLock;
import com.coffeeorder.config.KafkaProducerConfig;
import com.coffeeorder.menu.entity.Menu;
import com.coffeeorder.menu.repository.MenuRepository;
import com.coffeeorder.order.dto.OrderCreateRequest;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.entity.OrderStatus;
import com.coffeeorder.order.event.OrderEvent;
import com.coffeeorder.order.outbox.entity.OutboxEvent;
import com.coffeeorder.order.outbox.repository.OutboxEventRepository;
import com.coffeeorder.order.repository.OrderItemRepository;
import com.coffeeorder.order.repository.OrderRepository;
import com.coffeeorder.point.entity.PointBalance;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.repository.PointBalanceRepository;
import com.coffeeorder.point.repository.PointHistoryRepository;
import com.coffeeorder.user.repository.UserRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
 * 6단계({@code order-events} 토픽 발행)는 SCRUM-78(E6-3 태스크4, Transactional Outbox 확장)에서
 * 재구성됐다. 이전(SCRUM-76/77)에는 {@code ApplicationEventPublisher.publishEvent(OrderEvent)}로
 * 애플리케이션 이벤트를 발행하고 {@code @TransactionalEventListener(AFTER_COMMIT)} 리스너가 커밋
 * 이후 직접 {@code kafkaTemplate.send(...)}를 호출했으나, 이 방식은 커밋이 이미 끝난 시점에 리스너
 * 내부에서 {@code send} 자체가 실패해도 재시도/영속 큐가 없다는 한계가 있었다
 * (docs/design/rollback-no-publish-check.md 6절). 이제는 {@code order()}가 포인트 차감·주문/
 * 주문항목 저장과 **같은 물리 트랜잭션 안에서** {@link OutboxEventRepository}에
 * {@link OutboxEvent}(상태 {@code PENDING})를 원자적으로 기록하고, 실제 Kafka 발행은 트랜잭션 밖에서
 * 별도로 폴링하는 {@code com.coffeeorder.order.outbox.OutboxRelay}가 재시도까지 포함해 전담한다
 * (docs/design/outbox-relay-design.md). 저장이 실패(롤백)하면 아웃박스 행도 함께 롤백되므로,
 * "결제 롤백 시 이벤트 미발행" AC는 이 설계에서도 그대로 성립한다({@code payload}는
 * {@link KafkaProducerConfig#orderEventObjectMapper()}와 동일한 직렬화 규약으로 만든 JSON 문자열).
 * <p>
 * 재점검 결과(E6-3 태스크1, docs/design/jira-manual.md 246행): {@code order()}는 클래스 레벨
 * {@code @Transactional(readOnly = true)}를 오버라이드하는 단일 {@code @Transactional} 메서드이고,
 * {@link RedisDistributedLock#executeWithLock}은 별도 스레드/트랜잭션을 열지 않고 호출 스레드의
 * 트랜잭션 안에서 동기 실행되며, 포인트 차감~주문/주문항목 저장 사이에 {@code REQUIRES_NEW} 등
 * 트랜잭션 경계를 끊는 코드나 self-invocation이 없다. 따라서 포인트 차감(잔액 갱신 + {@code USE}
 * 이력 insert)부터 {@code orders}/{@code order_items} 저장(이제 {@link OutboxEvent} 저장 포함)까지가
 * 물리적으로 하나의 트랜잭션이며, 어느 단계든 실패 시 전체 롤백됨을
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
	private static final String OUTBOX_AGGREGATE_TYPE_ORDER = "ORDER";

	private final OrderRepository orderRepository;
	private final OrderItemRepository orderItemRepository;
	private final UserRepository userRepository;
	private final MenuRepository menuRepository;
	private final PointBalanceRepository pointBalanceRepository;
	private final PointHistoryRepository pointHistoryRepository;
	private final OutboxEventRepository outboxEventRepository;
	private final RedisDistributedLock redisDistributedLock;
	private final ObjectMapper orderEventObjectMapper;

	public OrderService(OrderRepository orderRepository,
			OrderItemRepository orderItemRepository,
			UserRepository userRepository,
			MenuRepository menuRepository,
			PointBalanceRepository pointBalanceRepository,
			PointHistoryRepository pointHistoryRepository,
			OutboxEventRepository outboxEventRepository,
			RedisDistributedLock redisDistributedLock) {
		this.orderRepository = orderRepository;
		this.orderItemRepository = orderItemRepository;
		this.userRepository = userRepository;
		this.menuRepository = menuRepository;
		this.pointBalanceRepository = pointBalanceRepository;
		this.pointHistoryRepository = pointHistoryRepository;
		this.outboxEventRepository = outboxEventRepository;
		this.redisDistributedLock = redisDistributedLock;
		this.orderEventObjectMapper = KafkaProducerConfig.orderEventObjectMapper();
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

		OrderEvent orderEvent = OrderEvent.from(order, orderItem);
		outboxEventRepository.save(new OutboxEvent(OUTBOX_AGGREGATE_TYPE_ORDER, order.getId(),
				KafkaProducerConfig.ORDER_EVENTS_TOPIC, serialize(orderEvent)));

		return new OrderResult(order, List.of(orderItem), balanceAfter);
	}

	/**
	 * {@link OrderEvent}를 Outbox {@code payload} 컬럼에 저장할 JSON 문자열로 직렬화한다.
	 * {@link OrderEvent}는 이 클래스가 항상 스스로 만드는 값 객체이므로 {@link JsonProcessingException}이
	 * 발생하는 경우는 사실상 없지만(필드가 모두 직렬화 가능한 기본 타입), 체크 예외를 언체크로 감싸
	 * 발생 시에도 트랜잭션이 기본 롤백 규칙에 따라 롤백되게 한다.
	 */
	private String serialize(OrderEvent orderEvent) {
		try {
			return orderEventObjectMapper.writeValueAsString(orderEvent);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException("OrderEvent 직렬화에 실패했습니다.", e);
		}
	}
}
