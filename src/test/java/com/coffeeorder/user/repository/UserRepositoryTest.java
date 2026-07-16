package com.coffeeorder.user.repository;

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
import com.coffeeorder.user.entity.User;

/**
 * {@link User} 엔티티와 {@link UserRepository}의 매핑을 검증하는 {@code @DataJpaTest} 슬라이스 테스트.
 * {@code application-test.yml}의 H2(MySQL 호환 모드) 데이터소스를 그대로 사용해 Flyway 마이그레이션
 * (V1__init_schema.sql/V2__seed_menu_user.sql)이 적용된 상태로 검증한다(docs/design/schema-strategy.md).
 * {@code @DataJpaTest}의 슬라이스 컴포넌트 스캔은 {@link JpaAuditingConfig}를 걸러낼 수 있어 명시적으로
 * {@code @Import}한다(JpaAuditingConfigTest 패턴 참고).
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
class UserRepositoryTest {

	@Autowired
	private UserRepository userRepository;

	@Test
	void 시드_데이터_3건을_findAll로_조회한다() {
		List<User> users = userRepository.findAll();

		assertThat(users).hasSize(3);
		assertThat(users).extracting(User::getName)
				.containsExactlyInAnyOrder("김민준", "이서연", "박도윤");
	}

	@Test
	void 신규_사용자_저장_시_createdAt_updatedAt이_자동으로_채워진다() {
		LocalDateTime beforeSave = LocalDateTime.now(ZoneOffset.UTC);

		User saved = userRepository.saveAndFlush(new User("최지우"));

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
