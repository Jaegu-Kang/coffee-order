package com.coffeeorder.point.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.coffeeorder.point.entity.PointHistory;

/**
 * {@link PointHistory} 영속성 리포지토리. 이번 서브태스크는 이력 저장(엔티티+리포지토리)만
 * 요구하므로 기본 {@code JpaRepository}의 저장 기능으로 충분하다. 이력 조회를 위한 커스텀 쿼리
 * 메서드는 이 리포지토리의 범위가 아니다(후속 서브태스크에서 다룬다).
 */
public interface PointHistoryRepository extends JpaRepository<PointHistory, Long> {
}
