package com.coffeeorder.order.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.coffeeorder.order.entity.Order;

/**
 * {@link Order} 영속성 리포지토리. 이번 티켓 범위는 엔티티/리포지토리 생성까지이므로
 * 인기 메뉴 집계 등에 필요한 커스텀 쿼리 메서드는 추가하지 않는다(별도 E5 티켓 범위).
 */
public interface OrderRepository extends JpaRepository<Order, Long> {
}
