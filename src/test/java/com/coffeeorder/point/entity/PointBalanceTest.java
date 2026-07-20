package com.coffeeorder.point.entity;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PointBalanceTest {

	@Test
	void charge_호출하면_잔액이_누적_증가한다() {
		PointBalance pointBalance = new PointBalance(1L, 1000L);

		pointBalance.charge(500L);
		pointBalance.charge(200L);

		assertThat(pointBalance.getBalance()).isEqualTo(1700L);
	}

	@Test
	void deduct_호출하면_잔액에서_정확히_차감된다() {
		PointBalance pointBalance = new PointBalance(1L, 1000L);

		pointBalance.deduct(300L);

		assertThat(pointBalance.getBalance()).isEqualTo(700L);
	}

	@Test
	void deduct는_잔액부족_검증을_하지_않으므로_음수_잔액도_그대로_반영한다() {
		PointBalance pointBalance = new PointBalance(1L, 100L);

		pointBalance.deduct(300L);

		assertThat(pointBalance.getBalance()).isEqualTo(-200L);
	}
}
