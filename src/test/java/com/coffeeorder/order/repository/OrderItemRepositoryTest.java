package com.coffeeorder.order.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.entity.OrderStatus;

/**
 * {@link OrderItem} 엔티티와 {@link OrderItemRepository}의 매핑을 검증하는 {@code @DataJpaTest} 슬라이스 테스트.
 * {@code order_items.order_id}/{@code menu_id}는 각각 {@code orders.id}/{@code menus.id}에 대한 FK 제약이
 * 있으므로, 부모 {@code Order}를 먼저 저장하고 {@code V2__seed_menu_user.sql}에 시드된
 * {@code menu_id}(1/2/3)를 사용한다(OrderRepositoryTest 패턴 참고).
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
class OrderItemRepositoryTest {

	@Autowired
	private OrderItemRepository orderItemRepository;

	@Autowired
	private OrderRepository orderRepository;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private Order saveParentOrder() {
		return orderRepository.saveAndFlush(
				new Order(1L, 6500L, OrderStatus.PAID, LocalDateTime.of(2026, 7, 16, 10, 30, 0)));
	}

	@Test
	void 신규_주문항목_저장_시_id가_채번되고_필드_값이_그대로_조회된다() {
		Order order = saveParentOrder();

		OrderItem saved = orderItemRepository.saveAndFlush(
				new OrderItem(order.getId(), 1L, "아메리카노", 3000L, 2));

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getOrderId()).isEqualTo(order.getId());
		assertThat(saved.getMenuId()).isEqualTo(1L);
		assertThat(saved.getMenuName()).isEqualTo("아메리카노");
		assertThat(saved.getUnitPrice()).isEqualTo(3000L);
		assertThat(saved.getQuantity()).isEqualTo(2);
	}

	@Test
	void 저장된_주문항목을_findById로_다시_조회할_수_있다() {
		Order order = saveParentOrder();

		OrderItem saved = orderItemRepository.saveAndFlush(
				new OrderItem(order.getId(), 2L, "카페라떼", 3500L, 1));

		OrderItem found = orderItemRepository.findById(saved.getId()).orElseThrow();

		assertThat(found.getOrderId()).isEqualTo(order.getId());
		assertThat(found.getMenuId()).isEqualTo(2L);
		assertThat(found.getMenuName()).isEqualTo("카페라떼");
		assertThat(found.getUnitPrice()).isEqualTo(3500L);
		assertThat(found.getQuantity()).isEqualTo(1);
	}

	@Test
	void 저장_이후_원본_메뉴의_가격과_이름이_바뀌어도_주문항목의_스냅샷은_변하지_않는다() {
		Order order = saveParentOrder();

		OrderItem saved = orderItemRepository.saveAndFlush(
				new OrderItem(order.getId(), 3L, "바닐라라떼", 4000L, 3));

		jdbcTemplate.update("UPDATE menus SET name = ?, price = ? WHERE id = ?",
				"바닐라라떼(변경)", 5000L, 3L);

		OrderItem found = orderItemRepository.findById(saved.getId()).orElseThrow();

		assertThat(found.getMenuName()).isEqualTo("바닐라라떼");
		assertThat(found.getUnitPrice()).isEqualTo(4000L);
	}
}
