package com.coffeeorder.order.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.test.context.ActiveProfiles;

import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.entity.OrderStatus;

/**
 * {@link OrderItemRepository#findPopularMenuCounts(LocalDateTime)}(인기 메뉴 집계 쿼리)를 검증하는
 * {@code @DataJpaTest} 슬라이스 테스트. {@code OrderStatus} enum에는 {@code PAID}만 존재해 "PAID 아님"
 * 케이스는 {@link JdbcTemplate}으로 {@code orders} 테이블에 직접 insert한다(OrderItemRepositoryTest의
 * JdbcTemplate 사용 패턴 참고). "현재 시각 - 168시간" 계산은 이 티켓 범위가 아니므로, 각 테스트는
 * 임의의 고정된 {@code from} 값을 직접 주입해 경계 로직만 검증한다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
class OrderItemRepositoryPopularMenuTest {

	@Autowired
	private OrderItemRepository orderItemRepository;

	@Autowired
	private OrderRepository orderRepository;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private Order savePaidOrder(LocalDateTime orderedAt) {
		return orderRepository.saveAndFlush(new Order(1L, 1000L, OrderStatus.PAID, orderedAt));
	}

	private void saveOrderItem(Long orderId, Long menuId, String menuName, Long unitPrice, int quantity) {
		orderItemRepository.saveAndFlush(new OrderItem(orderId, menuId, menuName, unitPrice, quantity));
	}

	/** {@code OrderStatus}에 없는 상태(예: CANCELED)의 주문을 저장하기 위해 JdbcTemplate으로 직접 insert한다. */
	private Long insertOrderWithStatus(String status, LocalDateTime orderedAt) {
		SimpleJdbcInsert insert = new SimpleJdbcInsert(jdbcTemplate)
				.withTableName("orders")
				.usingGeneratedKeyColumns("id");
		Map<String, Object> params = Map.of(
				"user_id", 1L,
				"total_amount", 1000L,
				"status", status,
				"ordered_at", orderedAt,
				"created_at", orderedAt);
		Number key = insert.executeAndReturnKey(params);
		return key.longValue();
	}

	/** top3 제한 검증을 위해 시드(1~3) 외 4번째 메뉴를 직접 insert하고 생성된 id를 반환한다. */
	private Long insertMenu(String name, Long price) {
		SimpleJdbcInsert insert = new SimpleJdbcInsert(jdbcTemplate)
				.withTableName("menus")
				.usingGeneratedKeyColumns("id");
		LocalDateTime now = LocalDateTime.of(2026, 7, 1, 0, 0, 0);
		Map<String, Object> params = Map.of(
				"name", name,
				"price", price,
				"created_at", now,
				"updated_at", now);
		Number key = insert.executeAndReturnKey(params);
		return key.longValue();
	}

	@Test
	void 여러_주문의_같은_메뉴_수량이_정확히_합산된다() {
		LocalDateTime from = LocalDateTime.of(2026, 7, 9, 0, 0, 0);

		Order order1 = savePaidOrder(from.plusDays(1));
		saveOrderItem(order1.getId(), 1L, "아메리카노", 3000L, 2);
		Order order2 = savePaidOrder(from.plusDays(2));
		saveOrderItem(order2.getId(), 1L, "아메리카노", 3000L, 3);

		List<PopularMenuCountProjection> result = orderItemRepository.findPopularMenuCounts(from);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getMenuId()).isEqualTo(1L);
		assertThat(result.get(0).getOrderCount()).isEqualTo(5L);
	}

	@Test
	void ordered_at이_from과_같은_주문은_포함되고_1초_이른_주문은_제외된다() {
		LocalDateTime from = LocalDateTime.of(2026, 7, 9, 0, 0, 0);

		Order includedOrder = savePaidOrder(from);
		saveOrderItem(includedOrder.getId(), 1L, "아메리카노", 3000L, 2);

		Order excludedOrder = savePaidOrder(from.minusSeconds(1));
		saveOrderItem(excludedOrder.getId(), 1L, "아메리카노", 3000L, 100);

		List<PopularMenuCountProjection> result = orderItemRepository.findPopularMenuCounts(from);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getMenuId()).isEqualTo(1L);
		assertThat(result.get(0).getOrderCount()).isEqualTo(2L);
	}

	@Test
	void PAID가_아닌_주문은_집계에서_제외된다() {
		LocalDateTime from = LocalDateTime.of(2026, 7, 9, 0, 0, 0);

		Long canceledOrderId = insertOrderWithStatus("CANCELED", from.plusDays(1));
		saveOrderItem(canceledOrderId, 2L, "카페라떼", 3500L, 10);

		Order paidOrder = savePaidOrder(from.plusDays(1));
		saveOrderItem(paidOrder.getId(), 2L, "카페라떼", 3500L, 3);

		List<PopularMenuCountProjection> result = orderItemRepository.findPopularMenuCounts(from);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getMenuId()).isEqualTo(2L);
		assertThat(result.get(0).getOrderCount()).isEqualTo(3L);
	}

	@Test
	void 합산_수량이_같으면_menu_id_오름차순으로_정렬된다() {
		LocalDateTime from = LocalDateTime.of(2026, 7, 9, 0, 0, 0);

		Order order1 = savePaidOrder(from.plusDays(1));
		saveOrderItem(order1.getId(), 2L, "카페라떼", 3500L, 4);
		Order order2 = savePaidOrder(from.plusDays(1));
		saveOrderItem(order2.getId(), 1L, "아메리카노", 3000L, 4);

		List<PopularMenuCountProjection> result = orderItemRepository.findPopularMenuCounts(from);

		assertThat(result).hasSize(2);
		assertThat(result.get(0).getMenuId()).isEqualTo(1L);
		assertThat(result.get(0).getOrderCount()).isEqualTo(4L);
		assertThat(result.get(1).getMenuId()).isEqualTo(2L);
		assertThat(result.get(1).getOrderCount()).isEqualTo(4L);
	}

	@Test
	void 서로_다른_4개_이상_메뉴가_집계_대상이면_상위_3개만_반환된다() {
		LocalDateTime from = LocalDateTime.of(2026, 7, 9, 0, 0, 0);
		Long fourthMenuId = insertMenu("바닐라라떼2", 4500L);

		Order order1 = savePaidOrder(from.plusDays(1));
		saveOrderItem(order1.getId(), 1L, "아메리카노", 3000L, 10);
		Order order2 = savePaidOrder(from.plusDays(1));
		saveOrderItem(order2.getId(), 2L, "카페라떼", 3500L, 9);
		Order order3 = savePaidOrder(from.plusDays(1));
		saveOrderItem(order3.getId(), 3L, "바닐라라떼", 4000L, 8);
		Order order4 = savePaidOrder(from.plusDays(1));
		saveOrderItem(order4.getId(), fourthMenuId, "바닐라라떼2", 4500L, 7);

		List<PopularMenuCountProjection> result = orderItemRepository.findPopularMenuCounts(from);

		assertThat(result).hasSize(3);
		assertThat(result.get(0).getMenuId()).isEqualTo(1L);
		assertThat(result.get(1).getMenuId()).isEqualTo(2L);
		assertThat(result.get(2).getMenuId()).isEqualTo(3L);
		assertThat(result).extracting(PopularMenuCountProjection::getMenuId)
				.doesNotContain(fourthMenuId);
	}
}
