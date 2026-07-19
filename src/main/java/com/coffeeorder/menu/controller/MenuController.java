package com.coffeeorder.menu.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.coffeeorder.menu.dto.MenuListResponse;
import com.coffeeorder.menu.dto.PopularMenuListResponse;
import com.coffeeorder.menu.service.MenuService;
import com.coffeeorder.menu.service.PopularMenuService;

/**
 * 메뉴 조회 API. {@code docs/api/menu.md}의 {@code GET /api/menus}, {@code GET /api/menus/popular}를
 * 제공한다.
 */
@RestController
@RequestMapping("/api/menus")
public class MenuController {

	private final MenuService menuService;
	private final PopularMenuService popularMenuService;

	public MenuController(MenuService menuService, PopularMenuService popularMenuService) {
		this.menuService = menuService;
		this.popularMenuService = popularMenuService;
	}

	@GetMapping
	public MenuListResponse getMenus() {
		return MenuListResponse.from(menuService.getMenus());
	}

	@GetMapping("/popular")
	public PopularMenuListResponse getPopularMenus() {
		return PopularMenuListResponse.from(popularMenuService.getPopularMenus());
	}
}
