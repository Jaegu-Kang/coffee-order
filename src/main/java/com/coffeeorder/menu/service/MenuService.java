package com.coffeeorder.menu.service;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.coffeeorder.menu.dto.MenuResponse;
import com.coffeeorder.menu.repository.MenuRepository;

/**
 * 메뉴 조회 비즈니스 로직. {@code docs/api/menu.md}의 {@code GET /api/menus}를 지원한다.
 * 엔티티({@link com.coffeeorder.menu.entity.Menu})를 직접 노출하지 않고
 * {@link MenuResponse}로 매핑하여 반환한다.
 */
@Service
@Transactional(readOnly = true)
public class MenuService {

	private final MenuRepository menuRepository;

	public MenuService(MenuRepository menuRepository) {
		this.menuRepository = menuRepository;
	}

	public List<MenuResponse> getMenus() {
		return menuRepository.findAll().stream()
				.map(MenuResponse::from)
				.toList();
	}
}
