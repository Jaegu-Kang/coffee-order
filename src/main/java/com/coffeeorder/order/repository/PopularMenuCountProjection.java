package com.coffeeorder.order.repository;

/**
 * 인기 메뉴 집계 쿼리({@link OrderItemRepository#findPopularMenuCounts})의 결과를 담는
 * Spring Data 인터페이스 프로젝션. {@code docs/api/menu.md}의 집계 개념 쿼리와 동일하게
 * {@code menu_id}, {@code SUM(quantity)}만 다루며, 메뉴명·가격 조립은 이 티켓 범위가 아니다
 * (후속 서브태스크인 {@code PopularMenuResponse} DTO에서 처리).
 */
public interface PopularMenuCountProjection {

	Long getMenuId();

	Long getOrderCount();
}
