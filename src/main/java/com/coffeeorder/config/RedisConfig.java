package com.coffeeorder.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Redis 연결/템플릿 인프라 설정(SCRUM-70 / E6-2 태스크1: "Redis 의존성 추가 + RedisConfig
 * 작성", docs/design/jira-manual.md 224~236행). build.gradle에 추가한
 * {@code spring-boot-starter-data-redis}가 자동 구성한 {@link RedisConnectionFactory}
 * (Lettuce, application-dev.yml의 {@code spring.data.redis.host}/{@code port})를 주입받아
 * {@link RedisTemplate} 빈 등록까지만 담당한다.
 * <p>
 * 분산락 유틸({@code point:{userId}} 키를 다루는 락 획득/해제 로직 자체)과 {@code PointBalance}
 * 비관적 락, 실제 락 적용은 후속 서브태스크(SCRUM-70 다음 태스크들, docs/design/jira-manual.md
 * 224~236행의 태스크2~4)의 범위다.
 * <p>
 * 이후 분산락 유틸이 {@code point:{userId}} 같은 문자열 키/값을 다루기 쉽도록 기본 JDK
 * 직렬화 대신 key/value 모두 {@link StringRedisSerializer}로 명시 설정한다.
 * 메인 애플리케이션 클래스가 아닌 별도 {@code @Configuration}으로 분리해 웹 슬라이스 테스트가
 * 영향받지 않도록 한다(docs/code-convention.md, JpaAuditingConfig 선례).
 */
@Configuration
public class RedisConfig {

	@Bean
	public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
		RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
		redisTemplate.setConnectionFactory(redisConnectionFactory);

		StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
		redisTemplate.setKeySerializer(stringRedisSerializer);
		redisTemplate.setValueSerializer(stringRedisSerializer);
		redisTemplate.setHashKeySerializer(stringRedisSerializer);
		redisTemplate.setHashValueSerializer(stringRedisSerializer);

		redisTemplate.afterPropertiesSet();
		return redisTemplate;
	}
}
