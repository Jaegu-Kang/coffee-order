package com.coffeeorder.config;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * {@link AuditingTestEntity} 저장을 위한 테스트 전용 리포지토리.
 */
interface AuditingTestEntityRepository extends JpaRepository<AuditingTestEntity, Long> {
}
