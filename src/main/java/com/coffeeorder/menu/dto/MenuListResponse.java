package com.coffeeorder.menu.dto;

import java.util.List;

/**
 * 메뉴 목록 응답 래퍼 DTO. {@code docs/api/menu.md}의 {@code GET /api/menus} 응답 형태
 * {@code {"menus": [...]}}에 대응한다.
 */
public class MenuListResponse {

	private final List<MenuResponse> menus;

	public MenuListResponse(List<MenuResponse> menus) {
		this.menus = menus;
	}

	public static MenuListResponse from(List<MenuResponse> menus) {
		return new MenuListResponse(menus);
	}

	public List<MenuResponse> getMenus() {
		return menus;
	}
}
