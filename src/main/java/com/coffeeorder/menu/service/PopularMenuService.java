package com.coffeeorder.menu.service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.coffeeorder.menu.dto.PopularMenuResponse;
import com.coffeeorder.menu.entity.Menu;
import com.coffeeorder.menu.repository.MenuRepository;
import com.coffeeorder.order.repository.OrderItemRepository;
import com.coffeeorder.order.repository.PopularMenuCountProjection;

/**
 * 인기 메뉴 조회 비즈니스 로직. {@code docs/api/menu.md}의 {@code GET /api/menus/popular}를 지원한다.
 * 최근 7일(168시간, 호출 시각 기준) {@code PAID} 주문을 대상으로 메뉴별 주문 수량 합계 상위 3개를 조회한다.
 */
@Service
@Transactional(readOnly = true)
public class PopularMenuService {

	private static final long POPULAR_MENU_PERIOD_HOURS = 168L;

	private final OrderItemRepository orderItemRepository;
	private final MenuRepository menuRepository;

	public PopularMenuService(OrderItemRepository orderItemRepository, MenuRepository menuRepository) {
		this.orderItemRepository = orderItemRepository;
		this.menuRepository = menuRepository;
	}

	public List<PopularMenuResponse> getPopularMenus() {
		LocalDateTime from = LocalDateTime.now(ZoneOffset.UTC).minusHours(POPULAR_MENU_PERIOD_HOURS);
		List<PopularMenuCountProjection> counts = orderItemRepository.findPopularMenuCounts(from);
		if (counts.isEmpty()) {
			return List.of();
		}

		List<Long> menuIds = counts.stream()
				.map(PopularMenuCountProjection::getMenuId)
				.toList();
		Map<Long, Menu> menusById = new HashMap<>();
		for (Menu menu : menuRepository.findAllById(menuIds)) {
			menusById.put(menu.getId(), menu);
		}

		return counts.stream()
				.filter(count -> menusById.containsKey(count.getMenuId()))
				.map(count -> PopularMenuResponse.from(menusById.get(count.getMenuId()), count.getOrderCount()))
				.toList();
	}
}
