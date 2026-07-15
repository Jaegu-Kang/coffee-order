package com.coffeeorder.config;

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

/**
 * {@link JpaAuditingConfig}가 저장 시 createdAt/updatedAt을 UTC 기준으로 자동 채우는지 검증하는
 * {@code @DataJpaTest} 슬라이스 테스트.
 * {@code application-test.yml}의 H2(MySQL 호환 모드) 데이터소스를 그대로 사용해야 Flyway
 * 마이그레이션(V1__init_schema.sql의 DATETIME 타입 등)이 정상 적용되므로, 기본 임베디드 DB
 * 치환을 비활성화한다(docs/design/schema-strategy.md: dev/test 동일 전략 유지).
 * {@code @DataJpaTest}의 슬라이스 컴포넌트 스캔 필터는 도메인과 무관한 일반 {@code @Configuration}을
 * 걸러낼 수 있어, {@link JpaAuditingConfig}를 명시적으로 {@code @Import}해 Auditing이 항상
 * 활성화되도록 한다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
class JpaAuditingConfigTest {

	@Autowired
	private AuditingTestEntityRepository auditingTestEntityRepository;

	@Test
	void 엔티티_저장_시_createdAt_updatedAt이_UTC_기준으로_자동_설정된다() {
		LocalDateTime beforeSave = LocalDateTime.now(ZoneOffset.UTC);

		AuditingTestEntity saved = auditingTestEntityRepository.saveAndFlush(new AuditingTestEntity("tester"));

		LocalDateTime afterSave = LocalDateTime.now(ZoneOffset.UTC);

		assertThat(saved.getCreatedAt()).isNotNull();
		assertThat(saved.getUpdatedAt()).isNotNull();
		assertThat(saved.getCreatedAt()).isEqualTo(saved.getUpdatedAt());
		assertThat(saved.getCreatedAt()).isBetween(
				beforeSave.minus(5, ChronoUnit.SECONDS),
				afterSave.plus(5, ChronoUnit.SECONDS));
	}
}
