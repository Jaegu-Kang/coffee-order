package com.coffeeorder.menu.dto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.coffeeorder.menu.entity.Menu;

class MenuResponseTest {

	@Test
	void from_메뉴_엔티티를_응답_DTO로_매핑한다() {
		Menu menu = new Menu("아메리카노", 3000L);

		MenuResponse response = MenuResponse.from(menu);

		assertThat(response.getId()).isEqualTo(menu.getId());
		assertThat(response.getName()).isEqualTo("아메리카노");
		assertThat(response.getPrice()).isEqualTo(3000L);
	}
}
