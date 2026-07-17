package com.coffeeorder.menu.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import com.coffeeorder.menu.dto.PopularMenuResponse;
import com.coffeeorder.menu.entity.Menu;
import com.coffeeorder.menu.repository.MenuRepository;
import com.coffeeorder.order.repository.OrderItemRepository;
import com.coffeeorder.order.repository.PopularMenuCountProjection;

@ExtendWith(MockitoExtension.class)
class PopularMenuServiceTest {

	@Mock
	private OrderItemRepository orderItemRepository;

	@Mock
	private MenuRepository menuRepository;

	@Test
	void getPopularMenus_집계_결과를_메뉴_정보와_함께_순서대로_매핑한다() {
		Menu americano = newMenu(1L, "아메리카노", 3000L);
		Menu latte = newMenu(2L, "카페라떼", 3500L);
		PopularMenuCountProjection firstCount = newProjection(1L, 42L);
		PopularMenuCountProjection secondCount = newProjection(2L, 20L);
		when(orderItemRepository.findPopularMenuCounts(any())).thenReturn(List.of(firstCount, secondCount));
		// findAllById는 순서를 보장하지 않으므로 일부러 프로젝션 순서와 반대로 반환한다.
		when(menuRepository.findAllById(List.of(1L, 2L))).thenReturn(List.of(latte, americano));

		PopularMenuService popularMenuService = new PopularMenuService(orderItemRepository, menuRepository);

		List<PopularMenuResponse> responses = popularMenuService.getPopularMenus();

		assertThat(responses).hasSize(2);
		assertThat(responses).extracting(PopularMenuResponse::getMenuId)
				.containsExactly(1L, 2L);
		assertThat(responses).extracting(PopularMenuResponse::getName)
				.containsExactly("아메리카노", "카페라떼");
		assertThat(responses).extracting(PopularMenuResponse::getOrderCount)
				.containsExactly(42L, 20L);
	}

	@Test
	void getPopularMenus_집계_결과가_없으면_빈_리스트를_반환한다() {
		when(orderItemRepository.findPopularMenuCounts(any())).thenReturn(Collections.emptyList());

		PopularMenuService popularMenuService = new PopularMenuService(orderItemRepository, menuRepository);

		List<PopularMenuResponse> responses = popularMenuService.getPopularMenus();

		assertThat(responses).isEmpty();
	}

	@Test
	void getPopularMenus_now_UTC_기준_168시간_전을_from으로_전달한다() {
		when(orderItemRepository.findPopularMenuCounts(any())).thenReturn(Collections.emptyList());

		PopularMenuService popularMenuService = new PopularMenuService(orderItemRepository, menuRepository);
		popularMenuService.getPopularMenus();

		ArgumentCaptor<LocalDateTime> fromCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
		verify(orderItemRepository).findPopularMenuCounts(fromCaptor.capture());

		LocalDateTime expected = LocalDateTime.now(ZoneOffset.UTC).minusHours(168L);
		Duration diff = Duration.between(fromCaptor.getValue(), expected).abs();
		assertThat(diff).isLessThan(Duration.ofSeconds(5));
	}

	private Menu newMenu(Long id, String name, Long price) {
		Menu menu = new Menu(name, price);
		ReflectionTestUtils.setField(menu, "id", id);
		return menu;
	}

	private PopularMenuCountProjection newProjection(Long menuId, Long orderCount) {
		return new PopularMenuCountProjection() {
			@Override
			public Long getMenuId() {
				return menuId;
			}

			@Override
			public Long getOrderCount() {
				return orderCount;
			}
		};
	}
}
