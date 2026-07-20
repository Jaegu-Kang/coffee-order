package com.coffeeorder.common.lock;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * {@link InMemoryRedisDistributedLock}을 {@link RedisDistributedLock} 빈으로 등록하는
 * 공용 테스트 설정(SCRUM-74). {@code PointServiceConcurrencyTest}/{@code OrderServiceConcurrencyTest}
 * 양쪽에서 {@code @Import}로 재사용해, 실제 Redis 없이 {@code PointService}/{@code OrderService}가
 * 요구하는 {@link RedisDistributedLock} 의존성을 채운다.
 */
@TestConfiguration
public class InMemoryRedisDistributedLockTestConfig {

	@Bean
	public RedisDistributedLock redisDistributedLock() {
		return new InMemoryRedisDistributedLock();
	}
}
