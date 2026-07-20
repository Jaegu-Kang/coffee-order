package com.coffeeorder.order.event;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import com.coffeeorder.config.KafkaProducerConfig;

/**
 * {@link OrderEvent}를 실제 Kafka {@code order-events} 토픽으로 발행하는 리스너
 * (SCRUM-76 / E6-3 태스크2). {@code OrderService.order()}가 트랜잭션 내부에서
 * {@code ApplicationEventPublisher.publishEvent(OrderEvent)}로 발행한 애플리케이션 이벤트를
 * {@link TransactionalEventListener}(phase = {@link TransactionPhase#AFTER_COMMIT})로 받아,
 * 트랜잭션이 실제로 커밋된 이후에만 {@code kafkaTemplate.send(...)}를 호출한다. 이로써
 * {@code docs/api/order.md} "처리" 6번("커밋 후(AFTER_COMMIT) 발행")과 실제 구현이 일치한다.
 * <p>
 * 롤백된 트랜잭션에서는 {@code AFTER_COMMIT} 리스너 자체가 호출되지 않으므로 Kafka로 발행되지
 * 않는다(이 동작을 직접 실증하는 통합 테스트는 태스크3 범위로 별도 진행). 발행 신뢰성 강화(Outbox
 * 등)는 태스크4 범위이며 이 클래스는 다루지 않는다.
 */
@Component
public class OrderEventKafkaListener {

	private final KafkaTemplate<String, Object> kafkaTemplate;

	public OrderEventKafkaListener(KafkaTemplate<String, Object> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
	public void onOrderEvent(OrderEvent event) {
		kafkaTemplate.send(KafkaProducerConfig.ORDER_EVENTS_TOPIC, event);
	}
}
