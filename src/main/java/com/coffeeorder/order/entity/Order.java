package com.coffeeorder.order.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

/**
 * 주문(결제 단위) 엔티티. {@code docs/db/schema.md}의 {@code orders} 테이블에 매핑된다.
 * {@code userId}는 코드베이스에 아직 {@code User} 엔티티가 없어 연관관계가 아닌 단순 FK 컬럼으로 둔다.
 * {@code orderedAt}은 주문이 발생한 비즈니스 시각으로, 감사용 {@code createdAt}(자동 관리)과
 * 달리 생성 시점에 명시적으로 전달받는다. {@code orders} 테이블에는 {@code updated_at} 컬럼이
 * 없으므로 이 엔티티에는 {@code updatedAt} 필드를 두지 않는다(docs/db/schema.md 확인).
 */
@Entity
@Table(name = "orders")
@EntityListeners(AuditingEntityListener.class)
public class Order {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "user_id", nullable = false)
	private Long userId;

	@Column(name = "total_amount", nullable = false)
	private Long totalAmount;

	@Enumerated(EnumType.STRING)
	@Column(name = "status", nullable = false, length = 20)
	private OrderStatus status;

	@Column(name = "ordered_at", nullable = false)
	private LocalDateTime orderedAt;

	@CreatedDate
	@Column(name = "created_at", nullable = false, updatable = false)
	private LocalDateTime createdAt;

	protected Order() {
	}

	public Order(Long userId, Long totalAmount, OrderStatus status, LocalDateTime orderedAt) {
		this.userId = userId;
		this.totalAmount = totalAmount;
		this.status = status;
		this.orderedAt = orderedAt;
	}

	public Long getId() {
		return id;
	}

	public Long getUserId() {
		return userId;
	}

	public Long getTotalAmount() {
		return totalAmount;
	}

	public OrderStatus getStatus() {
		return status;
	}

	public LocalDateTime getOrderedAt() {
		return orderedAt;
	}

	public LocalDateTime getCreatedAt() {
		return createdAt;
	}
}
