package com.coffeeorder.menu.dto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.coffeeorder.menu.entity.Menu;

class PopularMenuResponseTest {

	@Test
	void from_메뉴_엔티티와_주문_횟수를_응답_DTO로_매핑한다() {
		Menu menu = new Menu("아메리카노", 3000L);

		PopularMenuResponse response = PopularMenuResponse.from(menu, 42L);

		assertThat(response.getMenuId()).isEqualTo(menu.getId());
		assertThat(response.getName()).isEqualTo("아메리카노");
		assertThat(response.getPrice()).isEqualTo(3000L);
		assertThat(response.getOrderCount()).isEqualTo(42L);
	}
}
