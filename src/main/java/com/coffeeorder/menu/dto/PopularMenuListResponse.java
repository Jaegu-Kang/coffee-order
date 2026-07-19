package com.coffeeorder.menu.dto;

import java.util.List;

/**
 * 인기 메뉴 목록 응답 래퍼 DTO. {@code docs/api/menu.md}의 {@code GET /api/menus/popular} 응답 형태
 * {@code {"popularMenus": [...]}}에 대응한다.
 */
public class PopularMenuListResponse {

	private final List<PopularMenuResponse> popularMenus;

	public PopularMenuListResponse(List<PopularMenuResponse> popularMenus) {
		this.popularMenus = popularMenus;
	}

	public static PopularMenuListResponse from(List<PopularMenuResponse> popularMenus) {
		return new PopularMenuListResponse(popularMenus);
	}

	public List<PopularMenuResponse> getPopularMenus() {
		return popularMenus;
	}
}
