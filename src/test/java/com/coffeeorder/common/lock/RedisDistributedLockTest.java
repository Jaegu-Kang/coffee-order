package com.coffeeorder.common.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;

/**
 * {@link RedisDistributedLock}을 실제 Redis 브로커 없이 {@link RedisTemplate}/
 * {@link ValueOperations}를 Mockito로 mock해 검증하는 순수 단위 테스트
 * ({@code RedisConfigTest}와 동일한 패턴, SCRUM-71 / E6-2 태스크2).
 */
class RedisDistributedLockTest {

	@SuppressWarnings("unchecked")
	private RedisTemplate<String, Object> newMockRedisTemplate(ValueOperations<String, Object> valueOperations) {
		RedisTemplate<String, Object> redisTemplate = mock(RedisTemplate.class);
		when(redisTemplate.opsForValue()).thenReturn(valueOperations);
		return redisTemplate;
	}

	@Test
	@SuppressWarnings("unchecked")
	void 락_획득에_성공하면_콜백_결과를_반환하고_종료_후_락_해제를_한_번_호출한다() {
		ValueOperations<String, Object> valueOperations = mock(ValueOperations.class);
		RedisTemplate<String, Object> redisTemplate = newMockRedisTemplate(valueOperations);
		when(valueOperations.setIfAbsent(anyString(), any(), any(Duration.class))).thenReturn(true);
		when(redisTemplate.execute(any(RedisScript.class), anyList(), any())).thenReturn(1L);

		RedisDistributedLock lock = new RedisDistributedLock(redisTemplate);

		String result = lock.executeWithLock(
				"point:1", Duration.ofMillis(100), Duration.ofSeconds(3), () -> "done");

		assertThat(result).isEqualTo("done");
		verify(redisTemplate, times(1)).execute(any(RedisScript.class), anyList(), any());
	}

	@Test
	@SuppressWarnings("unchecked")
	void waitTime_내에_락을_획득하지_못하면_CONCURRENCY_CONFLICT_예외를_던지고_콜백과_해제는_실행하지_않는다() {
		ValueOperations<String, Object> valueOperations = mock(ValueOperations.class);
		RedisTemplate<String, Object> redisTemplate = newMockRedisTemplate(valueOperations);
		when(valueOperations.setIfAbsent(anyString(), any(), any(Duration.class))).thenReturn(false);

		RedisDistributedLock lock = new RedisDistributedLock(redisTemplate);
		AtomicBoolean callbackExecuted = new AtomicBoolean(false);

		assertThatThrownBy(() -> lock.executeWithLock(
				"point:1", Duration.ofMillis(10), Duration.ofSeconds(3), () -> {
					callbackExecuted.set(true);
					return "done";
				}))
				.isInstanceOf(BusinessException.class)
				.extracting(exception -> ((BusinessException) exception).getErrorCode())
				.isEqualTo(ErrorCode.CONCURRENCY_CONFLICT);

		assertThat(callbackExecuted).isFalse();
		verify(redisTemplate, never()).execute(any(RedisScript.class), anyList(), any());
	}

	@Test
	@SuppressWarnings("unchecked")
	void 콜백_실행_중_예외가_발생해도_원래_예외가_그대로_전파되고_락은_반드시_해제된다() {
		ValueOperations<String, Object> valueOperations = mock(ValueOperations.class);
		RedisTemplate<String, Object> redisTemplate = newMockRedisTemplate(valueOperations);
		when(valueOperations.setIfAbsent(anyString(), any(), any(Duration.class))).thenReturn(true);
		when(redisTemplate.execute(any(RedisScript.class), anyList(), any())).thenReturn(1L);

		RedisDistributedLock lock = new RedisDistributedLock(redisTemplate);
		IllegalStateException callbackException = new IllegalStateException("boom");

		assertThatThrownBy(() -> lock.executeWithLock(
				"point:1", Duration.ofMillis(100), Duration.ofSeconds(3), () -> {
					throw callbackException;
				}))
				.isSameAs(callbackException);

		verify(redisTemplate, times(1)).execute(any(RedisScript.class), anyList(), any());
	}

	@Test
	@SuppressWarnings("unchecked")
	void 락_획득_시_leaseTime을_TTL로_그대로_전달한다() {
		ValueOperations<String, Object> valueOperations = mock(ValueOperations.class);
		RedisTemplate<String, Object> redisTemplate = newMockRedisTemplate(valueOperations);
		when(valueOperations.setIfAbsent(anyString(), any(), any(Duration.class))).thenReturn(true);
		when(redisTemplate.execute(any(RedisScript.class), anyList(), any())).thenReturn(1L);

		RedisDistributedLock lock = new RedisDistributedLock(redisTemplate);
		Duration leaseTime = Duration.ofSeconds(5);

		lock.executeWithLock("point:1", Duration.ofMillis(100), leaseTime, () -> "done");

		verify(valueOperations).setIfAbsent(eq("point:1"), any(), eq(leaseTime));
	}

	@Test
	@SuppressWarnings("unchecked")
	void TTL_만료_후_다른_소유자가_재획득한_락은_해제_시_삭제하지_않는다() {
		// SET NX PX + 토큰 비교 후 삭제(CAS)를 실제 Redis EVAL과 동일한 의미로 흉내 내는
		// in-memory 스텁 저장소. redis.call('get')==ARGV[1]일 때만 del이 수행되는 Lua
		// 스크립트(RELEASE_SCRIPT)의 원자적 비교-삭제 동작을 검증하기 위함이다.
		Map<String, Object> store = new ConcurrentHashMap<>();
		ValueOperations<String, Object> valueOperations = mock(ValueOperations.class);
		RedisTemplate<String, Object> redisTemplate = newMockRedisTemplate(valueOperations);

		when(valueOperations.setIfAbsent(anyString(), any(), any(Duration.class)))
				.thenAnswer(invocation -> {
					String key = invocation.getArgument(0);
					Object value = invocation.getArgument(1);
					return store.putIfAbsent(key, value) == null;
				});
		when(redisTemplate.execute(any(RedisScript.class), anyList(), any()))
				.thenAnswer(invocation -> {
					List<String> keys = invocation.getArgument(1);
					Object token = invocation.getArgument(2);
					String key = keys.get(0);
					if (token.equals(store.get(key))) {
						store.remove(key);
						return 1L;
					}
					return 0L;
				});

		RedisDistributedLock lock = new RedisDistributedLock(redisTemplate);
		String otherOwnerToken = "other-owner-token";

		lock.executeWithLock("point:1", Duration.ofMillis(100), Duration.ofMillis(10), () -> {
			// 콜백 실행 중 TTL이 만료되고 다른 소유자가 같은 키를 재획득한 상황을 흉내 낸다.
			store.put("point:1", otherOwnerToken);
			return "done";
		});

		assertThat(store.get("point:1")).isEqualTo(otherOwnerToken);
	}
}
