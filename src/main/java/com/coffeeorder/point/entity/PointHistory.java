package com.coffeeorder.point.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

/**
 * 포인트 이력 엔티티. {@code docs/db/schema.md}의 {@code point_histories} 테이블에 매핑된다.
 * {@code type}은 {@code VARCHAR(10)}(CHARGE / USE) 컬럼으로, 이번 티켓(SCRUM-55)은 주문 결제
 * 차감({@link #TYPE_USE})만 다루므로 문자열 상수로만 정의한다(충전(CHARGE) 로직은 별도 E3 티켓
 * 범위이며 과설계 방지를 위해 enum은 도입하지 않는다). {@code point_histories} 테이블에는
 * {@code updated_at} 컬럼이 없으므로 이 엔티티에는 감사 필드로 {@code createdAt}만 둔다.
 */
@Entity
@Table(name = "point_histories")
@EntityListeners(AuditingEntityListener.class)
public class PointHistory {

	public static final String TYPE_USE = "USE";

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "user_id", nullable = false)
	private Long userId;

	@Column(name = "type", nullable = false, length = 10)
	private String type;

	@Column(name = "amount", nullable = false)
	private Long amount;

	@Column(name = "balance_after", nullable = false)
	private Long balanceAfter;

	@CreatedDate
	@Column(name = "created_at", nullable = false, updatable = false)
	private LocalDateTime createdAt;

	protected PointHistory() {
	}

	public PointHistory(Long userId, String type, Long amount, Long balanceAfter) {
		this.userId = userId;
		this.type = type;
		this.amount = amount;
		this.balanceAfter = balanceAfter;
	}

	public Long getId() {
		return id;
	}

	public Long getUserId() {
		return userId;
	}

	public String getType() {
		return type;
	}

	public Long getAmount() {
		return amount;
	}

	public Long getBalanceAfter() {
		return balanceAfter;
	}

	public LocalDateTime getCreatedAt() {
		return createdAt;
	}
}
