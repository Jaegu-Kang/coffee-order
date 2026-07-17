package com.coffeeorder.menu.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.coffeeorder.menu.entity.Menu;

class PopularMenuListResponseTest {

	@Test
	void from_전달받은_리스트를_그대로_popularMenus로_반환한다() {
		List<PopularMenuResponse> popularMenus = List.of(
				PopularMenuResponse.from(new Menu("아메리카노", 3000L), 42L),
				PopularMenuResponse.from(new Menu("카페라떼", 3500L), 20L));

		PopularMenuListResponse response = PopularMenuListResponse.from(popularMenus);

		assertThat(response.getPopularMenus()).isEqualTo(popularMenus);
	}
}
