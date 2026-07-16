package com.coffeeorder.point.entity;

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
 * 포인트 이력 엔티티. {@code docs/db/schema.md}의 {@code point_histories} 테이블에 매핑된다.
 * {@code userId}는 {@code users.id}를 참조하는 FK 컬럼이지만 {@code PointBalance}와 동일하게
 * 연관관계 매핑 없이 단순 컬럼으로 유지한다(엔티티 그래프 로딩 부작용 방지). {@code createdAt}만
 * {@link com.coffeeorder.config.JpaAuditingConfig}를 통해 자동 관리되며(docs/policy.md 감사 규칙),
 * 이 테이블에는 {@code updated_at}이 존재하지 않는다(이력은 불변 기록이므로 갱신 없음).
 * 잔액 증감 비즈니스 로직(충전/차감)은 이 엔티티의 범위가 아니다(후속 서브태스크에서 다룬다).
 */
@Entity
@Table(name = "point_histories")
@EntityListeners(AuditingEntityListener.class)
public class PointHistory {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "user_id", nullable = false)
	private Long userId;

	@Enumerated(EnumType.STRING)
	@Column(name = "type", nullable = false, length = 10)
	private PointHistoryType type;

	@Column(name = "amount", nullable = false)
	private Long amount;

	@Column(name = "balance_after", nullable = false)
	private Long balanceAfter;

	@CreatedDate
	@Column(name = "created_at", nullable = false, updatable = false)
	private LocalDateTime createdAt;

	protected PointHistory() {
	}

	public PointHistory(Long userId, PointHistoryType type, Long amount, Long balanceAfter) {
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

	public PointHistoryType getType() {
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
