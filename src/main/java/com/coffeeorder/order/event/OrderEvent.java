package com.coffeeorder.order.event;

import java.time.Instant;
import java.time.ZoneOffset;

import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;

/**
 * Kafka 토픽 {@code order-events}로 발행될 주문 결제 완료 이벤트의 메시지 스키마
 * (SCRUM-59 / E4-2 태스크2: "OrderEvent 메시지 스키마 정의 (orderId,userId,menuId,amount,orderedAt)",
 * docs/design/jira-manual.md 169행). 실제 {@code kafkaTemplate.send(...)} 발행 로직은
 * 태스크3(docs/design/jira-manual.md 170행) 범위이며 이 클래스는 스키마 정의만 담당한다.
 * <p>
 * <b>패키지 배치 근거</b>: {@code docs/code-convention.md}의 표준 계층(controller/service/
 * repository/entity/dto/exception)에는 없는 {@code order.event} 서브패키지를 신설했다.
 * 이 클래스는 애플리케이션 내부에서만 쓰이는 요청/응답 DTO가 아니라 Kafka로 나가는 외부 시스템
 * 계약(데이터 수집 플랫폼)이라는 점에서 {@code dto}와 성격이 다르며, {@code docs/design/coffee-order.md}가
 * order 도메인 레이어를 "order (+ Kafka producer, TransactionalEventListener)"로 설명하는 것과
 * 부합하도록 {@code event} 서브패키지로 분리했다.
 * <p>
 * <b>필드 계약 근거(docs/api/order.md "Kafka 이벤트 — 토픽 order-events" JSON 예시)</b>:
 * <pre>
 * { "orderId": 1001, "userId": 1, "menuId": 2, "amount": 3500, "orderedAt": "2026-07-13T10:00:00Z" }
 * </pre>
 * 필드명·개수(5개: orderId, userId, menuId, amount, orderedAt)가 위 예시와 정확히 일치해야 한다.
 * {@code orderId/userId/menuId/amount}는 예시가 정수 리터럴이며 DB 식별자·금액 규모를 고려해
 * {@link Long}으로 정의한다({@code Order}/{@code OrderItem} 엔티티의 동일 필드 타입과도 일치).
 * {@code orderedAt}은 예시가 {@code "2026-07-13T10:00:00Z"}처럼 UTC를 뜻하는 {@code Z} 접미사를
 * 포함하므로 {@link Instant}로 정의한다. {@code Order.orderedAt}은 {@link java.time.LocalDateTime}
 * (docs/policy.md "저장은 UTC" 원칙에 따른 UTC naive 값)이므로 그대로 재사용하면 직렬화 시 {@code Z}가
 * 붙지 않아 명세와 어긋난다({@link #from(Order, OrderItem)}에서 {@link ZoneOffset#UTC}를 명시해 변환).
 * <p>
 * 엔티티({@link Order}, {@link OrderItem})를 외부 계약에 직접 노출하지 않기 위해 필요한 필드만
 * 별도로 담는다(docs/code-convention.md "엔티티 미노출" 원칙).
 * <p>
 * SCRUM-76(E6-3 태스크2)부터는 {@code OrderService.order()}가 이 클래스를
 * {@code org.springframework.context.ApplicationEventPublisher}로 발행하는 내부 스프링 애플리케이션
 * 이벤트 payload로도 재사용한다. 실제 Kafka 발행은
 * {@link com.coffeeorder.order.event.OrderEventKafkaListener}가
 * {@code @TransactionalEventListener(phase = AFTER_COMMIT)}로 이 이벤트를 받아 담당하므로, 이 클래스
 * 자체의 스키마·필드 계약(외부 시스템 계약)은 변경되지 않는다.
 */
public class OrderEvent {

	private final Long orderId;

	private final Long userId;

	private final Long menuId;

	private final Long amount;

	private final Instant orderedAt;

	public OrderEvent(Long orderId, Long userId, Long menuId, Long amount, Instant orderedAt) {
		this.orderId = orderId;
		this.userId = userId;
		this.menuId = menuId;
		this.amount = amount;
		this.orderedAt = orderedAt;
	}

	/**
	 * 결제 완료된 {@link Order}와 그 주문의 {@link OrderItem}으로부터 {@link OrderEvent}를 만든다.
	 * 현재 {@code OrderService.order}는 한 주문에 메뉴 하나만 담으므로(docs/api/order.md 요청 바디가
	 * {@code menuId} 단일 값) {@code order.getTotalAmount()}가 곧 해당 메뉴 결제 금액과 같다.
	 * {@code orderedAt}은 {@code Order.orderedAt}(UTC naive {@link java.time.LocalDateTime})을
	 * 다른 타임존으로 오인하지 않도록 {@link ZoneOffset#UTC}를 명시해 {@link Instant}로 변환한다.
	 */
	public static OrderEvent from(Order order, OrderItem item) {
		return new OrderEvent(
				order.getId(),
				order.getUserId(),
				item.getMenuId(),
				order.getTotalAmount(),
				order.getOrderedAt().toInstant(ZoneOffset.UTC));
	}

	public Long getOrderId() {
		return orderId;
	}

	public Long getUserId() {
		return userId;
	}

	public Long getMenuId() {
		return menuId;
	}

	public Long getAmount() {
		return amount;
	}

	public Instant getOrderedAt() {
		return orderedAt;
	}
}
