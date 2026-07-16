package com.coffeeorder.order.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderStatus;

/**
 * {@link Order} 엔티티와 {@link OrderRepository}의 매핑을 검증하는 {@code @DataJpaTest} 슬라이스 테스트.
 * {@code orders.user_id}는 {@code users.id}에 대한 FK 제약이 있으므로 반드시
 * {@code V2__seed_menu_user.sql}에 시드된 {@code user_id}(1/2/3)를 사용한다(MenuRepositoryTest 패턴 참고).
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
class OrderRepositoryTest {

	@Autowired
	private OrderRepository orderRepository;

	@Test
	void 신규_주문_저장_시_id가_채번되고_필드_값이_그대로_조회된다() {
		LocalDateTime orderedAt = LocalDateTime.of(2026, 7, 16, 10, 30, 0);
		LocalDateTime beforeSave = LocalDateTime.now(ZoneOffset.UTC);

		Order saved = orderRepository.saveAndFlush(
				new Order(1L, 6500L, OrderStatus.PAID, orderedAt));

		LocalDateTime afterSave = LocalDateTime.now(ZoneOffset.UTC);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getUserId()).isEqualTo(1L);
		assertThat(saved.getTotalAmount()).isEqualTo(6500L);
		assertThat(saved.getStatus()).isEqualTo(OrderStatus.PAID);
		assertThat(saved.getOrderedAt()).isEqualTo(orderedAt);
		assertThat(saved.getCreatedAt()).isNotNull();
		assertThat(saved.getCreatedAt()).isBetween(
				beforeSave.minus(5, ChronoUnit.SECONDS),
				afterSave.plus(5, ChronoUnit.SECONDS));
	}

	@Test
	void 저장된_주문을_findById로_다시_조회할_수_있다() {
		LocalDateTime orderedAt = LocalDateTime.of(2026, 7, 16, 11, 0, 0);
		Order saved = orderRepository.saveAndFlush(
				new Order(2L, 3000L, OrderStatus.PAID, orderedAt));

		Order found = orderRepository.findById(saved.getId()).orElseThrow();

		assertThat(found.getUserId()).isEqualTo(2L);
		assertThat(found.getTotalAmount()).isEqualTo(3000L);
		assertThat(found.getStatus()).isEqualTo(OrderStatus.PAID);
		assertThat(found.getOrderedAt()).isEqualTo(orderedAt);
	}
}
