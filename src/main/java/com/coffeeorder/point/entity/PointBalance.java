package com.coffeeorder.point.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

/**
 * 포인트 잔액 엔티티. {@code docs/db/schema.md}의 {@code point_balances} 테이블에 매핑된다.
 * PK가 {@code user_id}(FK→users.id)로, {@code IDENTITY} 채번이 아닌 사용자 식별값을 그대로
 * 사용한다. {@code point_balances} 테이블에는 {@code created_at} 컬럼이 없으므로 이 엔티티에는
 * {@code createdAt} 필드를 두지 않는다(docs/db/schema.md 확인).
 * <p>
 * 이번 티켓(SCRUM-55)은 주문 시 잔액 차감에 필요한 최소 조회·차감 기능만 다루며, 스키마상
 * "행 단위 비관적 락"·{@code version} 낙관적 락 병행은 도전 과제(E6) 범위이므로 여기서는
 * 기본 {@code findById()} 조회만 사용한다(향후 락 적용을 쉽게 하기 위해 커스텀 쿼리는 두지 않음).
 */
@Entity
@Table(name = "point_balances")
@EntityListeners(AuditingEntityListener.class)
public class PointBalance {

	@Id
	@Column(name = "user_id")
	private Long userId;

	@Column(name = "balance", nullable = false)
	private Long balance;

	@Column(name = "version", nullable = false)
	private Long version;

	@LastModifiedDate
	@Column(name = "updated_at", nullable = false)
	private LocalDateTime updatedAt;

	protected PointBalance() {
	}

	public PointBalance(Long userId, Long balance) {
		this.userId = userId;
		this.balance = balance;
		this.version = 0L;
	}

	/**
	 * 잔액을 차감한다. 잔액 부족 여부 판정(INSUFFICIENT_POINT)은 서비스 계층 책임이며,
	 * 이 메서드는 이미 검증된 차감액만 반영한다.
	 */
	public void deduct(Long amount) {
		this.balance -= amount;
		this.version += 1;
	}

	public Long getUserId() {
		return userId;
	}

	public Long getBalance() {
		return balance;
	}

	public Long getVersion() {
		return version;
	}

	public LocalDateTime getUpdatedAt() {
		return updatedAt;
	}
}
