package com.coffeeorder.menu.dto;

import com.coffeeorder.menu.entity.Menu;

/**
 * 메뉴 응답 DTO. {@code docs/api/menu.md}의 {@code GET /api/menus} 응답 필드에 대응한다.
 * 엔티티({@link Menu})를 직접 노출하지 않고 필요한 필드만 매핑한다.
 */
public class MenuResponse {

	private final Long id;
	private final String name;
	private final Long price;

	public MenuResponse(Long id, String name, Long price) {
		this.id = id;
		this.name = name;
		this.price = price;
	}

	public static MenuResponse from(Menu menu) {
		return new MenuResponse(menu.getId(), menu.getName(), menu.getPrice());
	}

	public Long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public Long getPrice() {
		return price;
	}
}
