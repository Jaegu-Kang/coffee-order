package com.coffeeorder.order.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import com.coffeeorder.config.KafkaProducerConfig;

/**
 * {@link OrderEventKafkaListener}의 Mockito 단위 테스트(SCRUM-76 / E6-3 태스크2,
 * docs/code-convention.md "서비스: Mockito 단위 테스트" 관례 적용). 실제
 * {@code @TransactionalEventListener(phase = AFTER_COMMIT)} 트랜잭션 커밋 후 호출 여부는
 * 스프링 트랜잭션 인프라 영역이므로, 이 테스트는 리스너 메서드에 {@link OrderEvent}를 직접
 * 전달했을 때 {@code kafkaTemplate.send(order-events, event)}가 정확히 호출되는지만 검증한다.
 */
@ExtendWith(MockitoExtension.class)
class OrderEventKafkaListenerTest {

	@Mock
	private KafkaTemplate<String, Object> kafkaTemplate;

	@Test
	void onOrderEvent_호출되면_order_events_토픽으로_이벤트를_그대로_발행한다() {
		OrderEventKafkaListener listener = new OrderEventKafkaListener(kafkaTemplate);
		OrderEvent event = new OrderEvent(1001L, 1L, 2L, 3500L, Instant.parse("2026-07-13T10:00:00Z"));

		listener.onOrderEvent(event);

		ArgumentCaptor<OrderEvent> eventCaptor = ArgumentCaptor.forClass(OrderEvent.class);
		verify(kafkaTemplate).send(eq(KafkaProducerConfig.ORDER_EVENTS_TOPIC), eventCaptor.capture());
		assertThat(eventCaptor.getValue()).isSameAs(event);
	}
}
