package com.coffeeorder.config;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

/**
 * JPA Auditing 설정.
 * 모든 영속 엔티티의 {@code createdAt}/{@code updatedAt}을 자동 관리한다(docs/policy.md 감사 규칙).
 * 메인 애플리케이션 클래스가 아닌 별도 {@code @Configuration}으로 분리해 웹 슬라이스 테스트가
 * 영향받지 않도록 한다(docs/code-convention.md).
 */
@Configuration
@EnableJpaAuditing(dateTimeProviderRef = "utcDateTimeProvider")
public class JpaAuditingConfig {

	/**
	 * 감사 컬럼 시각을 JVM 기본 타임존에 의존하지 않고 UTC 기준으로 고정한다(docs/policy.md
	 * "시간/날짜: 저장은 UTC 기준" 규칙).
	 */
	@Bean
	public DateTimeProvider utcDateTimeProvider() {
		return () -> Optional.of(LocalDateTime.now(ZoneOffset.UTC));
	}
}
