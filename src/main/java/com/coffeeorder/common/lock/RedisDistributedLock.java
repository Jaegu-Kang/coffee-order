package com.coffeeorder.common.lock;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Supplier;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;

/**
 * Redis 기반 범용 분산락 유틸(SCRUM-71 / E6-2 태스크2: "분산락 유틸 작성",
 * docs/design/jira-manual.md 224~236행). 이 클래스는 호출자가 넘긴 임의의 문자열 키에 대해
 * SET NX PX(= {@link ValueOperations#setIfAbsent(Object, Object, Duration)}) 방식으로
 * 락을 걸고 콜백을 실행한 뒤 반드시 해제하는 범용 로직만 담당한다. {@code "point:"} 같은
 * 접두어를 하드코딩하지 않으며, 실제 {@code point:{userId}} 키 조합·{@code PointService}/
 * {@code OrderService} 적용·{@code PointBalance}에 대한 {@code @Lock(PESSIMISTIC_WRITE)}
 * 적용은 후속 서브태스크(SCRUM-71 다음 태스크3·4)의 범위다.
 * <p>
 * 락 대기 시간({@code waitTime})과 락 유지 시간/TTL({@code leaseTime})을 서로 다른
 * 파라미터로 분리해 받는다. {@code waitTime}은 "얼마나 폴링하며 기다리다가 포기할지"(빠른
 * 실패)를, {@code leaseTime}은 "락을 잡은 인스턴스가 죽더라도 영원히 잠기지 않도록 하는
 * TTL"(데드락 방지)을 의미하며 서로 다른 목적을 가지므로 하나로 합치지 않는다.
 * {@code waitTime} 내에 끝내 락을 획득하지 못하면 새 에러코드를 만들지 않고 기존
 * {@link ErrorCode#CONCURRENCY_CONFLICT}로 {@link BusinessException}을 던진다.
 * <p>
 * 락 획득 시 고유 토큰(UUID)을 값으로 저장하고, 콜백 실행을 try-finally로 감싸 정상
 * 종료·예외 종료 어느 경우에도 해제를 시도한다. 해제는 "자신이 건 락인지"를 저장된
 * 토큰과 원자적으로 비교한 뒤 삭제하는 Lua(EVAL) 스크립트로 수행해, TTL 만료 후 다른
 * 소유자가 재획득한 락을 실수로 지우는 것을 방지한다.
 */
@Component
public class RedisDistributedLock {

	private static final long RETRY_INTERVAL_MILLIS = 50L;

	private static final RedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>(
			"if redis.call('get', KEYS[1]) == ARGV[1] then "
					+ "return redis.call('del', KEYS[1]) "
					+ "else "
					+ "return 0 "
					+ "end",
			Long.class);

	private final RedisTemplate<String, Object> redisTemplate;

	public RedisDistributedLock(RedisTemplate<String, Object> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	/**
	 * {@code key}에 대한 락을 획득한 뒤 {@code task}를 실행하고, 정상/예외 종료 어느 경우에도
	 * 락을 해제한다.
	 *
	 * @param key       잠글 키(예: {@code point:{userId}}. 접두어 조합은 호출부 책임)
	 * @param waitTime  락 획득을 위해 폴링하며 대기할 최대 시간(빠른 실패 기준)
	 * @param leaseTime 락을 유지할 최대 시간(TTL, 데드락 방지 기준)
	 * @param task      락을 잡은 상태에서 실행할 작업
	 * @return {@code task}의 실행 결과
	 * @throws BusinessException {@code waitTime} 내에 락을 획득하지 못한 경우
	 *                            ({@link ErrorCode#CONCURRENCY_CONFLICT})
	 */
	public <T> T executeWithLock(String key, Duration waitTime, Duration leaseTime, Supplier<T> task) {
		String token = UUID.randomUUID().toString();

		if (!tryAcquire(key, token, waitTime, leaseTime)) {
			throw new BusinessException(ErrorCode.CONCURRENCY_CONFLICT);
		}

		try {
			return task.get();
		} finally {
			release(key, token);
		}
	}

	private boolean tryAcquire(String key, String token, Duration waitTime, Duration leaseTime) {
		ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
		long deadlineMillis = System.currentTimeMillis() + waitTime.toMillis();

		while (true) {
			Boolean acquired = valueOperations.setIfAbsent(key, token, leaseTime);
			if (Boolean.TRUE.equals(acquired)) {
				return true;
			}
			if (System.currentTimeMillis() >= deadlineMillis) {
				return false;
			}
			sleep(RETRY_INTERVAL_MILLIS);
		}
	}

	private void release(String key, String token) {
		redisTemplate.execute(RELEASE_SCRIPT, Collections.singletonList(key), token);
	}

	private void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
