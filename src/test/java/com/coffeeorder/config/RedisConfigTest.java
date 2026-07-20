package com.coffeeorder.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * {@link RedisConfig}를 실제 Redis 브로커/스프링 컨텍스트 없이 직접 인스턴스화해 검증하는
 * 순수 단위 테스트({@link KafkaProducerConfigTest}와 동일한 패턴). 실제 Redis 연결을 사용한
 * 검증(락 획득/해제 등)은 후속 서브태스크(분산락 유틸) 범위다.
 */
class RedisConfigTest {

	private final RedisConfig redisConfig = new RedisConfig();

	@Test
	void redisTemplate은_전달받은_ConnectionFactory를_그대로_사용한다() {
		RedisConnectionFactory redisConnectionFactory = mock(RedisConnectionFactory.class);

		RedisTemplate<String, Object> redisTemplate = redisConfig.redisTemplate(redisConnectionFactory);

		assertThat(redisTemplate.getConnectionFactory()).isSameAs(redisConnectionFactory);
	}

	@Test
	void redisTemplate의_key_value_직렬화는_문자열_직렬화_방식이다() {
		RedisConnectionFactory redisConnectionFactory = mock(RedisConnectionFactory.class);

		RedisTemplate<String, Object> redisTemplate = redisConfig.redisTemplate(redisConnectionFactory);

		assertThat(redisTemplate.getKeySerializer()).isInstanceOf(StringRedisSerializer.class);
		assertThat(redisTemplate.getValueSerializer()).isInstanceOf(StringRedisSerializer.class);
		assertThat(redisTemplate.getHashKeySerializer()).isInstanceOf(StringRedisSerializer.class);
		assertThat(redisTemplate.getHashValueSerializer()).isInstanceOf(StringRedisSerializer.class);
	}
}
