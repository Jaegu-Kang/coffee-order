package com.coffeeorder.menu.dto;

import com.coffeeorder.menu.entity.Menu;

/**
 * 인기 메뉴 응답 DTO. {@code docs/api/menu.md}의 {@code GET /api/menus/popular} 응답
 * {@code popularMenus} 배열 원소({@code menuId}, {@code name}, {@code price}, {@code orderCount})에
 * 대응한다. 엔티티({@link Menu})를 직접 노출하지 않고 필요한 필드만 매핑하며, 집계값인
 * {@code orderCount}는 별도 파라미터로 전달받아 조립한다.
 */
public class PopularMenuResponse {

	private final Long menuId;
	private final String name;
	private final Long price;
	private final Long orderCount;

	public PopularMenuResponse(Long menuId, String name, Long price, Long orderCount) {
		this.menuId = menuId;
		this.name = name;
		this.price = price;
		this.orderCount = orderCount;
	}

	public static PopularMenuResponse from(Menu menu, Long orderCount) {
		return new PopularMenuResponse(menu.getId(), menu.getName(), menu.getPrice(), orderCount);
	}

	public Long getMenuId() {
		return menuId;
	}

	public String getName() {
		return name;
	}

	public Long getPrice() {
		return price;
	}

	public Long getOrderCount() {
		return orderCount;
	}
}
