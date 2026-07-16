package com.coffeeorder.point.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.coffeeorder.point.entity.PointHistory;

/**
 * {@link PointHistory} 영속성 리포지토리. 이번 티켓(SCRUM-55) 범위는 주문 결제 시
 * 차감 이력 저장(save)뿐이므로 사용자별 이력 조회 등 커스텀 쿼리 메서드는 추가하지 않는다.
 */
public interface PointHistoryRepository extends JpaRepository<PointHistory, Long> {
}
