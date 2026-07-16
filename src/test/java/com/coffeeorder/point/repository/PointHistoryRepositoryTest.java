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

import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.point.entity.PointHistory;
import com.coffeeorder.point.entity.PointHistoryType;

/**
 * {@link PointHistory} 엔티티와 {@link PointHistoryRepository}의 매핑을 검증하는
 * {@code @DataJpaTest} 슬라이스 테스트.
 * {@code application-test.yml}의 H2(MySQL 호환 모드) 데이터소스를 그대로 사용해 Flyway 마이그레이션
 * (V1__init_schema.sql/V2__seed_menu_user.sql)이 적용된 상태로 검증한다(docs/design/schema-strategy.md).
 * {@code @DataJpaTest}의 슬라이스 컴포넌트 스캔은 {@link JpaAuditingConfig}를 걸러낼 수 있어 명시적으로
 * {@code @Import}한다(PointBalanceRepositoryTest 패턴 참고).
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
class PointHistoryRepositoryTest {

	@Autowired
	private PointHistoryRepository pointHistoryRepository;

	@Test
	void 시드된_사용자를_참조하는_이력_저장_시_id가_자동_채번되고_필드값이_정상_저장된다() {
		PointHistory saved = pointHistoryRepository.saveAndFlush(
				new PointHistory(1L, PointHistoryType.CHARGE, 1000L, 1000L));

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getUserId()).isEqualTo(1L);
		assertThat(saved.getType()).isEqualTo(PointHistoryType.CHARGE);
		assertThat(saved.getAmount()).isEqualTo(1000L);
		assertThat(saved.getBalanceAfter()).isEqualTo(1000L);
	}

	@Test
	void 신규_이력_저장_시_createdAt이_자동으로_채워진다() {
		LocalDateTime beforeSave = LocalDateTime.now(ZoneOffset.UTC);

		PointHistory saved = pointHistoryRepository.saveAndFlush(
				new PointHistory(2L, PointHistoryType.CHARGE, 500L, 500L));

		LocalDateTime afterSave = LocalDateTime.now(ZoneOffset.UTC);

		assertThat(saved.getCreatedAt()).isNotNull();
		assertThat(saved.getCreatedAt()).isBetween(
				beforeSave.minus(5, ChronoUnit.SECONDS),
				afterSave.plus(5, ChronoUnit.SECONDS));
	}

	@Test
	void type이_CHARGE와_USE_각각_왕복_저장_조회_시_정확히_매핑된다() {
		PointHistory chargeSaved = pointHistoryRepository.saveAndFlush(
				new PointHistory(3L, PointHistoryType.CHARGE, 2000L, 2000L));
		PointHistory useSaved = pointHistoryRepository.saveAndFlush(
				new PointHistory(3L, PointHistoryType.USE, 300L, 1700L));

		PointHistory chargeFound = pointHistoryRepository.findById(chargeSaved.getId()).orElseThrow();
		PointHistory useFound = pointHistoryRepository.findById(useSaved.getId()).orElseThrow();

		assertThat(chargeFound.getType()).isEqualTo(PointHistoryType.CHARGE);
		assertThat(useFound.getType()).isEqualTo(PointHistoryType.USE);
	}

	@Test
	void 존재하지_않는_사용자를_참조하면_FK_제약_위반으로_예외가_발생한다() {
		assertThatThrownBy(() -> pointHistoryRepository.saveAndFlush(
				new PointHistory(999L, PointHistoryType.CHARGE, 1000L, 1000L)))
				.isInstanceOf(RuntimeException.class);
	}
}
