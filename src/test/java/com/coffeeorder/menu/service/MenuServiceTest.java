package com.coffeeorder.menu.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coffeeorder.menu.dto.MenuResponse;
import com.coffeeorder.menu.entity.Menu;
import com.coffeeorder.menu.repository.MenuRepository;

@ExtendWith(MockitoExtension.class)
class MenuServiceTest {

	@Mock
	private MenuRepository menuRepository;

	@Test
	void getMenus_전체_메뉴를_응답_DTO_리스트로_매핑한다() {
		Menu americano = new Menu("아메리카노", 3000L);
		Menu latte = new Menu("카페라떼", 3500L);
		when(menuRepository.findAll()).thenReturn(List.of(americano, latte));

		MenuService menuService = new MenuService(menuRepository);

		List<MenuResponse> responses = menuService.getMenus();

		assertThat(responses).hasSize(2);
		assertThat(responses).extracting(MenuResponse::getName)
				.containsExactly("아메리카노", "카페라떼");
		assertThat(responses).extracting(MenuResponse::getPrice)
				.containsExactly(3000L, 3500L);
	}

	@Test
	void getMenus_메뉴가_없으면_빈_리스트를_반환한다() {
		when(menuRepository.findAll()).thenReturn(Collections.emptyList());

		MenuService menuService = new MenuService(menuRepository);

		List<MenuResponse> responses = menuService.getMenus();

		assertThat(responses).isEmpty();
	}
}
