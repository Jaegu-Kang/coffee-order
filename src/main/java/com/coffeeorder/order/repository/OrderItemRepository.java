package com.coffeeorder.order.repository;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.coffeeorder.order.entity.OrderItem;

/**
 * {@link OrderItem} 영속성 리포지토리. {@code docs/api/menu.md}의 인기 메뉴 집계 개념 쿼리를
 * {@link #findPopularMenuCounts(LocalDateTime)}로 반영한다("현재 시각 - 168시간" 계산 자체는
 * 이 리포지토리의 책임이 아니며, 호출하는 서비스 계층이 {@code from}을 계산해 전달한다).
 */
public interface OrderItemRepository extends JpaRepository<OrderItem, Long> {

	/**
	 * 최근 7일(호출 측이 전달한 {@code from} 기준) {@code PAID} 주문을 대상으로 메뉴별 주문 수량
	 * 합계 상위 3개를 반환한다. 동점 시 {@code menu_id} 오름차순으로 정렬한다
	 * (docs/api/menu.md § GET /api/menus/popular 집계 쿼리 참고).
	 */
	@Query(value = "SELECT oi.menu_id AS menuId, SUM(oi.quantity) AS orderCount "
			+ "FROM order_items oi JOIN orders o ON o.id = oi.order_id "
			+ "WHERE o.status = 'PAID' AND o.ordered_at >= :from "
			+ "GROUP BY oi.menu_id "
			+ "ORDER BY orderCount DESC, oi.menu_id ASC "
			+ "LIMIT 3", nativeQuery = true)
	List<PopularMenuCountProjection> findPopularMenuCounts(@Param("from") LocalDateTime from);
}
