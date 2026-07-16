package com.coffeeorder.point.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.point.entity.PointBalance;

/**
 * {@link PointBalance} 엔티티와 {@link PointBalanceRepository}의 매핑을 검증하는
 * {@code @DataJpaTest} 슬라이스 테스트.
 * {@code application-test.yml}의 H2(MySQL 호환 모드) 데이터소스를 그대로 사용해 Flyway 마이그레이션
 * (V1__init_schema.sql/V2__seed_menu_user.sql)이 적용된 상태로 검증한다(docs/design/schema-strategy.md).
 * {@code @DataJpaTest}의 슬라이스 컴포넌트 스캔은 {@link JpaAuditingConfig}를 걸러낼 수 있어 명시적으로
 * {@code @Import}한다(JpaAuditingConfigTest 패턴 참고).
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
class PointBalanceRepositoryTest {

	@Autowired
	private PointBalanceRepository pointBalanceRepository;

	@Test
	void 시드된_사용자를_참조하는_잔액_저장_시_balance_기본값은_0이다() {
		PointBalance saved = pointBalanceRepository.saveAndFlush(new PointBalance(1L));

		assertThat(saved.getUserId()).isEqualTo(1L);
		assertThat(saved.getBalance()).isEqualTo(0L);
	}

	@Test
	void 신규_잔액_저장_시_updatedAt이_자동으로_채워진다() {
		LocalDateTime beforeSave = LocalDateTime.now(ZoneOffset.UTC);

		PointBalance saved = pointBalanceRepository.saveAndFlush(new PointBalance(2L));

		LocalDateTime afterSave = LocalDateTime.now(ZoneOffset.UTC);

		assertThat(saved.getUpdatedAt()).isNotNull();
		assertThat(saved.getUpdatedAt()).isBetween(
				beforeSave.minus(5, ChronoUnit.SECONDS),
				afterSave.plus(5, ChronoUnit.SECONDS));
	}

	@Test
	void version은_0으로_시작하고_변경_후_저장하면_증가한다() {
		PointBalance saved = pointBalanceRepository.saveAndFlush(new PointBalance(3L));
		assertThat(saved.getVersion()).isEqualTo(0L);

		ReflectionTestUtils.setField(saved, "balance", 1000L);
		PointBalance updated = pointBalanceRepository.saveAndFlush(saved);

		assertThat(updated.getVersion()).isEqualTo(1L);
		assertThat(updated.getBalance()).isEqualTo(1000L);
	}

	@Test
	void 존재하지_않는_사용자를_참조하면_FK_제약_위반으로_예외가_발생한다() {
		assertThatThrownBy(() -> pointBalanceRepository.saveAndFlush(new PointBalance(999L)))
				.isInstanceOf(RuntimeException.class);
	}
}
