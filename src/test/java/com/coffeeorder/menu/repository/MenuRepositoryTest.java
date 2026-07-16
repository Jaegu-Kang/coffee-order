package com.coffeeorder.menu.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.menu.entity.Menu;

/**
 * {@link Menu} 엔티티와 {@link MenuRepository}의 매핑을 검증하는 {@code @DataJpaTest} 슬라이스 테스트.
 * {@code application-test.yml}의 H2(MySQL 호환 모드) 데이터소스를 그대로 사용해 Flyway 마이그레이션
 * (V1__init_schema.sql/V2__seed_menu_user.sql)이 적용된 상태로 검증한다(docs/design/schema-strategy.md).
 * {@code @DataJpaTest}의 슬라이스 컴포넌트 스캔은 {@link JpaAuditingConfig}를 걸러낼 수 있어 명시적으로
 * {@code @Import}한다(JpaAuditingConfigTest 패턴 참고).
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
class MenuRepositoryTest {

	@Autowired
	private MenuRepository menuRepository;

	@Test
	void 시드_데이터_3건을_findAll로_조회한다() {
		List<Menu> menus = menuRepository.findAll();

		assertThat(menus).hasSize(3);
		assertThat(menus).extracting(Menu::getName)
				.containsExactlyInAnyOrder("아메리카노", "카페라떼", "바닐라라떼");
		assertThat(menus).filteredOn(menu -> menu.getName().equals("아메리카노"))
				.first()
				.satisfies(menu -> assertThat(menu.getPrice()).isEqualTo(3000L));
		assertThat(menus).filteredOn(menu -> menu.getName().equals("카페라떼"))
				.first()
				.satisfies(menu -> assertThat(menu.getPrice()).isEqualTo(3500L));
		assertThat(menus).filteredOn(menu -> menu.getName().equals("바닐라라떼"))
				.first()
				.satisfies(menu -> assertThat(menu.getPrice()).isEqualTo(4000L));
	}

	@Test
	void 신규_메뉴_저장_시_createdAt_updatedAt이_자동으로_채워진다() {
		LocalDateTime beforeSave = LocalDateTime.now(ZoneOffset.UTC);

		Menu saved = menuRepository.saveAndFlush(new Menu("에스프레소", 2500L));

		LocalDateTime afterSave = LocalDateTime.now(ZoneOffset.UTC);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getCreatedAt()).isNotNull();
		assertThat(saved.getUpdatedAt()).isNotNull();
		assertThat(saved.getCreatedAt()).isEqualTo(saved.getUpdatedAt());
		assertThat(saved.getCreatedAt()).isBetween(
				beforeSave.minus(5, ChronoUnit.SECONDS),
				afterSave.plus(5, ChronoUnit.SECONDS));
	}
}
