package com.coffeeorder.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import com.coffeeorder.common.exception.BusinessException;
import com.coffeeorder.common.exception.ErrorCode;
import com.coffeeorder.common.lock.RedisDistributedLock;
import com.coffeeorder.config.KafkaProducerConfig;
import com.coffeeorder.order.outbox.entity.OutboxEvent;
import com.coffeeorder.order.outbox.entity.OutboxEventStatus;
import com.coffeeorder.order.outbox.repository.OutboxEventRepository;

/**
 * {@link OutboxRelay}의 Mockito 단위 테스트(E6-3 태스크4, SCRUM-78, docs/code-convention.md
 * "서비스: Mockito 단위 테스트" 관례 적용 — DB/Redis/Kafka 브로커 없이 로직만 검증). Step4 성공 기준
 * (a)(b)(c)를 모두 다룬다: (a) 발행 성공 시 {@code kafkaTemplate.send} 1회 호출 + 행 SENT 전이,
 * (b) 발행 실패 시 예외가 전파되지 않고 상태/재시도 카운트만 갱신, (c) 이미 SENT인 행은 폴링 쿼리
 * 조건({@code status = PENDING}) 자체에서 제외됨(리포지토리 스텁으로 실증) + 락 획득 실패 시 조용히
 * 스킵.
 */
@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

	private static final String RELAY_LOCK_KEY = "outbox-relay";
	private static final String PAYLOAD_TEMPLATE = "{\"orderId\":%d,\"userId\":1,\"menuId\":2,\"amount\":3500,"
			+ "\"orderedAt\":\"2026-07-13T10:00:00Z\"}";

	@Mock
	private OutboxEventRepository outboxEventRepository;

	@Mock
	private KafkaTemplate<String, Object> kafkaTemplate;

	@Mock
	private RedisDistributedLock redisDistributedLock;

	private OutboxRelay outboxRelay;

	@BeforeEach
	void setUp() {
		outboxRelay = new OutboxRelay(outboxEventRepository, kafkaTemplate, redisDistributedLock);
	}

	/** {@code executeWithLock}이 실제 락 없이 콜백({@code Supplier})을 즉시 실행하도록 스텁한다(OrderServiceTest 패턴 재사용). */
	@SuppressWarnings("unchecked")
	private void stubLockToRunImmediately() {
		when(redisDistributedLock.executeWithLock(eq(RELAY_LOCK_KEY), any(Duration.class), any(Duration.class), any()))
				.thenAnswer(invocation -> invocation.getArgument(3, Supplier.class).get());
	}

	private OutboxEvent pendingEvent(Long id, Long orderId) {
		OutboxEvent event = new OutboxEvent("ORDER", orderId, KafkaProducerConfig.ORDER_EVENTS_TOPIC,
				String.format(PAYLOAD_TEMPLATE, orderId));
		ReflectionTestUtils.setField(event, "id", id);
		return event;
	}

	@Test
	@SuppressWarnings("unchecked")
	void relayPendingEvents_발행에_성공하면_kafkaTemplate_send를_1회_호출하고_행을_SENT로_전이시킨다() {
		stubLockToRunImmediately();
		OutboxEvent event = pendingEvent(1L, 1001L);
		when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Pageable.class)))
				.thenReturn(List.of(event));
		SendResult<String, Object> sendResult = mock(SendResult.class);
		when(kafkaTemplate.send(eq(KafkaProducerConfig.ORDER_EVENTS_TOPIC), any()))
				.thenReturn(CompletableFuture.completedFuture(sendResult));

		outboxRelay.relayPendingEvents();

		ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
		verify(kafkaTemplate).send(eq(KafkaProducerConfig.ORDER_EVENTS_TOPIC), payloadCaptor.capture());
		Map<String, Object> sentPayload = (Map<String, Object>) payloadCaptor.getValue();
		assertThat(((Number) sentPayload.get("orderId")).longValue()).isEqualTo(1001L);

		ArgumentCaptor<OutboxEvent> savedCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
		verify(outboxEventRepository).save(savedCaptor.capture());
		assertThat(savedCaptor.getValue()).isSameAs(event);
		assertThat(savedCaptor.getValue().getStatus()).isEqualTo(OutboxEventStatus.SENT);
		assertThat(savedCaptor.getValue().getSentAt()).isNotNull();
		assertThat(savedCaptor.getValue().getRetryCount()).isZero();
	}

	@Test
	void relayPendingEvents_발행에_실패하면_예외를_전파하지_않고_PENDING_상태를_유지한_채_retryCount만_증가시킨다() {
		stubLockToRunImmediately();
		OutboxEvent event = pendingEvent(2L, 1002L);
		when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Pageable.class)))
				.thenReturn(List.of(event));
		when(kafkaTemplate.send(eq(KafkaProducerConfig.ORDER_EVENTS_TOPIC), any()))
				.thenThrow(new RuntimeException("Kafka 발행 실패(테스트)"));

		assertThatCode(() -> outboxRelay.relayPendingEvents()).doesNotThrowAnyException();

		ArgumentCaptor<OutboxEvent> savedCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
		verify(outboxEventRepository).save(savedCaptor.capture());
		assertThat(savedCaptor.getValue().getStatus()).isEqualTo(OutboxEventStatus.PENDING);
		assertThat(savedCaptor.getValue().getRetryCount()).isEqualTo(1);
	}

	@Test
	void relayPendingEvents_PENDING_행이_없으면_kafkaTemplate을_호출하지_않는다() {
		stubLockToRunImmediately();
		when(outboxEventRepository.findByStatusOrderByCreatedAtAsc(eq(OutboxEventStatus.PENDING), any(Pageable.class)))
				.thenReturn(List.of());

		outboxRelay.relayPendingEvents();

		verify(kafkaTemplate, never()).send(any(), any());
		verify(outboxEventRepository, never()).save(any());
	}

	@Test
	void relayPendingEvents_락_획득에_실패하면_예외를_전파하지_않고_배치_처리를_건너뛴다() {
		when(redisDistributedLock.executeWithLock(eq(RELAY_LOCK_KEY), any(Duration.class), any(Duration.class), any()))
				.thenThrow(new BusinessException(ErrorCode.CONCURRENCY_CONFLICT));

		assertThatCode(() -> outboxRelay.relayPendingEvents()).doesNotThrowAnyException();

		verifyNoInteractions(outboxEventRepository, kafkaTemplate);
	}
}
