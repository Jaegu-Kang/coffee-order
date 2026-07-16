package com.coffeeorder.point.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

/**
 * 포인트 잔액 엔티티. {@code docs/db/schema.md}의 {@code point_balances} 테이블에 매핑된다.
 * PK({@code user_id})는 {@code users.id}를 공유하는 FK이므로 {@code @GeneratedValue}를 사용하지
 * 않는다(IDENTITY 전략 아님). {@code updatedAt}만
 * {@link com.coffeeorder.config.JpaAuditingConfig}를 통해 자동 관리되며(docs/policy.md 감사 규칙),
 * 이 테이블에는 {@code created_at}이 존재하지 않는다.
 * {@code version}은 낙관적 락 병행용 컬럼이다. 잔액 증가는 {@link #charge(Long)}로 캡슐화되어
 * 있으며, 비관적 락/Redis 분산락을 통한 동시성 제어는 이 엔티티의 범위가 아니다(후속 서브태스크에서 다룬다).
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

	@Version
	@Column(name = "version", nullable = false)
	private Long version;

	@LastModifiedDate
	@Column(name = "updated_at", nullable = false)
	private LocalDateTime updatedAt;

	protected PointBalance() {
	}

	public PointBalance(Long userId) {
		this.userId = userId;
		this.balance = 0L;
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

	/**
	 * 잔액을 {@code amount}만큼 증가시킨다. 호출자(서비스 계층)가 이미 양수임을 검증한 금액만
	 * 전달한다고 가정하며, 이 메서드는 음수/0에 대한 방어 검증을 중복 수행하지 않는다.
	 */
	public void charge(Long amount) {
		this.balance += amount;
	}
}
