package com.coffeeorder.order.outbox;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.common.lock.RedisDistributedLock;
import com.coffeeorder.config.KafkaProducerConfig;
import com.coffeeorder.order.outbox.entity.OutboxEvent;
import com.coffeeorder.order.outbox.entity.OutboxEventStatus;
import com.coffeeorder.order.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * {@code outbox_events} 테이블을 폴링해 실제 Kafka 발행을 전담하는 릴레이(E6-3 태스크4, SCRUM-78
 * "(확장) Transactional Outbox 테이블 + 릴레이 설계/구현"). {@code OrderService.order()}가 남긴
 * {@link OutboxEventStatus#PENDING} 행을 오래된 순으로 배치 조회해 {@link KafkaTemplate}로 발행을
 * 시도하고, 성공하면 {@link OutboxEventStatus#SENT}로, 실패하면 {@code retryCount}만 증가시켜
 * {@link OutboxEventStatus#PENDING} 상태를 유지한다(다음 폴링에서 자동 재시도 — at-least-once,
 * 최대 재시도·수동 개입 정책은 이 확장의 범위 밖이다. {@link OutboxEventStatus} Javadoc 참고).
 * <p>
 * <b>트랜잭션 경계</b>: 이 클래스는 별도 {@code @Transactional} 메서드를 두지 않는다.
 * {@link OutboxEventRepository}(Spring Data JPA {@code SimpleJpaRepository})의 {@code save}/
 * {@code findByStatusOrderByCreatedAtAsc}는 그 자체로 각각 독립된 트랜잭션에서 실행되므로, 한 행의
 * Kafka 발행 성공/실패가 다른 행의 처리에 영향을 주지 않는다. 만약 이 클래스에 자체
 * {@code @Transactional} 메서드를 추가해 {@link #relayPendingEvents()}(스프링 프록시를 거치는
 * 진입점) 내부 람다에서 {@code this.xxx()}로 호출하면 self-invocation으로 그 애너테이션이
 * 무시되므로(OrderService 관련 문서·{@code transaction-atomicity-check.md} (e)항과 동일한 함정),
 * 의도적으로 트랜잭션 경계를 리포지토리 메서드 단위로 좁게 유지했다.
 * <p>
 * <b>다중 인스턴스 중복 발행 방지</b>: 새 락 유틸을 만들지 않고 기존 {@link RedisDistributedLock}을
 * 고정 키({@value #RELAY_LOCK_KEY})로 재사용해, 한 번에 한 인스턴스만 폴링 배치를 처리하게 한다.
 * {@code waitTime}을 짧게(폴링 주기보다 훨씬 짧게) 잡아 "락을 얻지 못하면 오래 기다리지 않고 이번
 * 폴링을 건너뛴다"는 정책을 구현했다. 락 획득 실패는
 * {@link BusinessException}({@link ErrorCode#CONCURRENCY_CONFLICT})으로 알려지므로, 이를
 * "다른 인스턴스가 처리 중"으로 간주해 조용히 스킵한다(다른 {@code ErrorCode}의
 * {@link BusinessException}은 예상치 못한 오류이므로 그대로 전파한다). 완전한 exactly-once는
 * 보장하지 않는다(at-least-once, 컨슈머 측 멱등 처리는 범위 밖).
 * <p>
 * <b>스케줄 트리거</b>: {@link Scheduled}는 이 클래스에 항상 붙어 있지만, 실제로 자동 실행되려면
 * {@code @EnableScheduling}이 컨텍스트에 등록되어야 한다. {@code test} 프로파일에서는
 * {@code com.coffeeorder.config.SchedulingConfig}가 비활성화되어 자동 트리거가 걸리지 않으므로,
 * 테스트는 {@link #relayPendingEvents()}를 직접 호출해 결정론적으로 검증한다.
 */
@Component
public class OutboxRelay {

	private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

	private static final String RELAY_LOCK_KEY = "outbox-relay";
	private static final Duration LOCK_WAIT_TIME = Duration.ofMillis(200);
	private static final Duration LOCK_LEASE_TIME = Duration.ofSeconds(10);
	private static final int BATCH_SIZE = 50;
	private static final long SEND_TIMEOUT_SECONDS = 5;

	private final OutboxEventRepository outboxEventRepository;
	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final RedisDistributedLock redisDistributedLock;
	private final ObjectMapper orderEventObjectMapper;

	public OutboxRelay(OutboxEventRepository outboxEventRepository,
			KafkaTemplate<String, Object> kafkaTemplate,
			RedisDistributedLock redisDistributedLock) {
		this.outboxEventRepository = outboxEventRepository;
		this.kafkaTemplate = kafkaTemplate;
		this.redisDistributedLock = redisDistributedLock;
		this.orderEventObjectMapper = KafkaProducerConfig.orderEventObjectMapper();
	}

	/**
	 * {@value #RELAY_LOCK_KEY} 락을 (짧게) 시도해 얻으면 {@link #relayBatch()}로 한 배치를 처리하고,
	 * 얻지 못하면 다른 인스턴스가 처리 중인 것으로 보고 조용히 스킵한다.
	 */
	@Scheduled(fixedDelayString = "${outbox.relay.fixed-delay-ms:1000}")
	public void relayPendingEvents() {
		try {
			redisDistributedLock.executeWithLock(RELAY_LOCK_KEY, LOCK_WAIT_TIME, LOCK_LEASE_TIME, () -> {
				relayBatch();
				return Boolean.TRUE;
			});
		} catch (BusinessException e) {
			if (e.getErrorCode() != ErrorCode.CONCURRENCY_CONFLICT) {
				throw e;
			}
			log.debug("다른 인스턴스가 Outbox 릴레이를 처리 중이라 이번 폴링을 건너뜁니다.");
		}
	}

	private void relayBatch() {
		List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatusOrderByCreatedAtAsc(
				OutboxEventStatus.PENDING, PageRequest.of(0, BATCH_SIZE));
		for (OutboxEvent outboxEvent : pendingEvents) {
			relayOne(outboxEvent);
		}
	}

	/**
	 * {@code outboxEvent} 한 건을 Kafka로 발행 시도한다. 성공하면 {@link OutboxEventStatus#SENT}로
	 * 전이시키고, 실패하면(직렬화 오류·전송 실패·타임아웃 등 어떤 예외든) 예외를 이 메서드 밖으로
	 * 전파하지 않고 {@link OutboxEvent#recordFailure()}만 호출해 다음 폴링에서 계속 재시도되게 한다
	 * (한 행의 실패가 배치의 다른 행 처리를 막지 않도록).
	 */
	private void relayOne(OutboxEvent outboxEvent) {
		try {
			Object payload = orderEventObjectMapper.readValue(outboxEvent.getPayload(), Map.class);
			kafkaTemplate.send(outboxEvent.getTopic(), payload).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			outboxEvent.markSent(LocalDateTime.now(ZoneOffset.UTC));
		} catch (Exception e) {
			log.warn("Outbox 이벤트(id={}) 발행 실패, 다음 폴링에서 재시도합니다.", outboxEvent.getId(), e);
			outboxEvent.recordFailure();
		}
		outboxEventRepository.save(outboxEvent);
	}
}
