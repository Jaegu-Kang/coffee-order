package com.coffeeorder.config;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

/**
 * {@link JpaAuditingConfig}의 감사 동작(createdAt/updatedAt 자동 설정)만 검증하기 위한
 * 테스트 전용 엔티티. 운영 엔티티가 아직 없으므로 기존 {@code users} 테이블
 * (id, name, created_at, updated_at)에 매핑해 Hibernate {@code ddl-auto: validate}와
 * 충돌 없이 검증한다. 프로덕션 코드에는 영향을 주지 않는다(test 소스 전용).
 */
@Entity
@Table(name = "users")
@EntityListeners(AuditingEntityListener.class)
class AuditingTestEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "name", nullable = false)
	private String name;

	@CreatedDate
	@Column(name = "created_at", nullable = false, updatable = false)
	private LocalDateTime createdAt;

	@LastModifiedDate
	@Column(name = "updated_at", nullable = false)
	private LocalDateTime updatedAt;

	protected AuditingTestEntity() {
	}

	AuditingTestEntity(String name) {
		this.name = name;
	}

	Long getId() {
		return id;
	}

	String getName() {
		return name;
	}

	LocalDateTime getCreatedAt() {
		return createdAt;
	}

	LocalDateTime getUpdatedAt() {
		return updatedAt;
	}
}
