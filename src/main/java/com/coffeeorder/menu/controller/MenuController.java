package com.coffeeorder.menu.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.coffeeorder.menu.dto.MenuListResponse;
import com.coffeeorder.menu.service.MenuService;

/**
 * 메뉴 조회 API. {@code docs/api/menu.md}의 {@code GET /api/menus}를 제공한다.
 */
@RestController
@RequestMapping("/api/menus")
public class MenuController {

	private final MenuService menuService;

	public MenuController(MenuService menuService) {
		this.menuService = menuService;
	}

	@GetMapping
	public MenuListResponse getMenus() {
		return MenuListResponse.from(menuService.getMenus());
	}
}
