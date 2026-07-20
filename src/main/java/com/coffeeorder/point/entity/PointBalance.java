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
 * PK가 {@code user_id}(FK→users.id)로, {@code IDENTITY} 채번이 아닌 사용자 식별값을 그대로
 * 사용한다. {@code point_balances} 테이블에는 {@code created_at} 컬럼이 없으므로 이 엔티티에는
 * {@code createdAt} 필드를 두지 않는다(docs/db/schema.md 확인).
 * <p>
 * 스키마상 "행 단위 비관적 락"·{@code version} 낙관적 락 병행은 도전 과제(E6) 범위다. 비관적 락 조회
 * 메서드({@link com.coffeeorder.point.repository.PointBalanceRepository#findByIdForUpdate(Long)})는
 * 이미 추가되었으나(SCRUM-72), 이 엔티티 자체는 락 적용 여부와 무관하게 변경되지 않으며 충전·차감·주문
 * 서비스 경로에서 해당 락 조회 메서드를 실제로 사용하도록 바꾸는 작업은 후속 서브태스크에서 다룬다.
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

	public PointBalance(Long userId, Long balance) {
		this.userId = userId;
		this.balance = balance;
	}

	public PointBalance(Long userId) {
		this(userId, 0L);
	}

	/**
	 * 잔액을 차감한다. 잔액 부족 여부 판정(INSUFFICIENT_POINT)은 서비스 계층 책임이며,
	 * 이 메서드는 이미 검증된 차감액만 반영한다.
	 */
	public void deduct(Long amount) {
		this.balance -= amount;
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
