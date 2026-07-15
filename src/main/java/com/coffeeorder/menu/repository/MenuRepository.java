package com.coffeeorder.menu.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.coffeeorder.menu.entity.Menu;

/**
 * {@link Menu} 영속성 리포지토리. 현재 요구되는 조회는 전체 목록 조회(GET /api/menus)뿐이므로
 * 기본 {@code JpaRepository}의 {@code findAll()}로 충분하다(커스텀 쿼리는 범위 밖).
 */
public interface MenuRepository extends JpaRepository<Menu, Long> {
}
