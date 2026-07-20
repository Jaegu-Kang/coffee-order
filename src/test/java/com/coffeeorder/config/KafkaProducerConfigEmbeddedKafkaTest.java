package com.coffeeorder.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.entity.OrderStatus;
import com.coffeeorder.order.event.OrderEvent;

/**
 * {@code order-events} 발행(SCRUM-61, {@code OrderService.order})이 실제 Kafka 프로토콜 위에서
 * 토픽명·JSON 스키마대로 나가는지 인메모리 브로커(EmbeddedKafka)로 검증하는 테스트
 * (SCRUM-62 / E4-2 태스크4, docs/design/jira-manual.md 171행).
 * <p>
 * 기존 {@link com.coffeeorder.order.service.OrderServiceTest}(Mockito
 * {@code verify(eventPublisher).publishEvent(...)}, SCRUM-76부터 실제 Kafka 발행은
 * {@link com.coffeeorder.order.event.OrderEventKafkaListener}가 {@code AFTER_COMMIT}에서 담당)와
 * {@link com.coffeeorder.order.event.OrderEventTest}(JSON 스키마 순수 단위 검증)는 각각 "발행 호출
 * 여부"와 "직렬화 결과"만 검증할 뿐, {@link KafkaProducerConfig#producerFactory()}가 실제로 구성한
 * {@link org.springframework.kafka.support.serializer.JsonSerializer}를 통해 브로커로 바이트가
 * 나가고 그 바이트가 다시 명세대로 소비되는 end-to-end 경로는 검증하지 않는다. 이 테스트는 그 갭을
 * 메운다.
 * <p>
 * {@link KafkaProducerConfigTest} 선례와 동일하게 "브로커/스프링 컨텍스트 최소화" 원칙을 유지하기 위해
 * {@code @SpringBootTest} 전체 컨텍스트를 띄우지 않는다. {@link EmbeddedKafka}는
 * {@code EmbeddedKafkaCondition}(순수 JUnit5 {@code Extension})으로 동작해 Spring 테스트 컨텍스트
 * 없이도 {@link EmbeddedKafkaBroker}를 테스트 생성자 파라미터로 주입받을 수 있다. 브로커/토픽 프로비저닝
 * 자체는 EmbeddedKafka가 대신하므로, {@code test} 프로파일에서 비활성화되는
 * {@link KafkaProducerConfig#orderEventsTopic()}/{@link KafkaProducerConfig#kafkaAdmin()}
 * (클래스 상단 주석 참고)은 이 테스트와 무관하다 — {@link KafkaProducerConfig}를 직접 인스턴스화해
 * {@link KafkaProducerConfig#producerFactory()}만 재사용하고, {@code bootstrapServers}만 EmbeddedKafka
 * 브로커 주소로 주입한다.
 */
@EmbeddedKafka(partitions = 1, topics = KafkaProducerConfig.ORDER_EVENTS_TOPIC)
class KafkaProducerConfigEmbeddedKafkaTest {

	private static final int ADMIN_OPERATION_TIMEOUT_SECONDS = 3;

	private static final String CONSUMER_GROUP_ID = "order-events-embedded-test";

	private static final Long USER_ID = 1L;

	private static final Long MENU_ID = 2L;

	private static final Long AMOUNT = 3500L;

	private final EmbeddedKafkaBroker embeddedKafkaBroker;

	private Consumer<String, String> consumer;

	KafkaProducerConfigEmbeddedKafkaTest(EmbeddedKafkaBroker embeddedKafkaBroker) {
		this.embeddedKafkaBroker = embeddedKafkaBroker;
	}

	@AfterEach
	void closeConsumer() {
		if (consumer != null) {
			consumer.close();
		}
	}

	@Test
	void KafkaTemplate으로_발행한_OrderEvent가_order_events_토픽에_명세와_동일한_JSON으로_실제_전달된다() throws Exception {
		KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig(embeddedKafkaBroker.getBrokersAsString(),
				ADMIN_OPERATION_TIMEOUT_SECONDS);
		ProducerFactory<String, Object> producerFactory = kafkaProducerConfig.producerFactory();
		KafkaTemplate<String, Object> kafkaTemplate = kafkaProducerConfig.kafkaTemplate(producerFactory);

		Order order = new Order(USER_ID, AMOUNT, OrderStatus.PAID, LocalDateTime.of(2026, 7, 13, 10, 0, 0));
		setId(order, 1001L);
		OrderItem orderItem = new OrderItem(order.getId(), MENU_ID, "카페라떼", AMOUNT, 1);
		OrderEvent orderEvent = OrderEvent.from(order, orderItem);

		kafkaTemplate.send(KafkaProducerConfig.ORDER_EVENTS_TOPIC, orderEvent).get(5, TimeUnit.SECONDS);

		consumer = createConsumer();
		ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer,
				KafkaProducerConfig.ORDER_EVENTS_TOPIC);

		assertThat(record.topic()).isEqualTo(KafkaProducerConfig.ORDER_EVENTS_TOPIC);

		// JsonSerializer가 실제로 사용한 것과 동일한 ObjectMapper 구성(KafkaProducerConfig.producerFactory()
		// 참고)으로 소비된 바이트를 역직렬화해, orderedAt이 epoch 숫자가 아닌 "...Z" ISO-8601 문자열로
		// 나갔는지까지 확인한다(OrderEventTest의 직렬화 계약과 동일 기준).
		ObjectMapper orderEventObjectMapper = JacksonUtils.enhancedObjectMapper()
				.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		Map<String, Object> asMap = orderEventObjectMapper.readValue(record.value(), Map.class);

		assertThat(asMap.keySet()).containsExactlyInAnyOrder("orderId", "userId", "menuId", "amount", "orderedAt");
		assertThat(asMap.get("orderId")).isEqualTo(orderEvent.getOrderId().intValue());
		assertThat(asMap.get("userId")).isEqualTo(orderEvent.getUserId().intValue());
		assertThat(asMap.get("menuId")).isEqualTo(orderEvent.getMenuId().intValue());
		assertThat(asMap.get("amount")).isEqualTo(orderEvent.getAmount().intValue());
		assertThat(asMap.get("orderedAt")).isEqualTo("2026-07-13T10:00:00Z");
	}

	private Consumer<String, String> createConsumer() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafkaBroker, CONSUMER_GROUP_ID,
				true);
		Consumer<String, String> newConsumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(),
				new StringDeserializer());
		embeddedKafkaBroker.consumeFromAnEmbeddedTopic(newConsumer, KafkaProducerConfig.ORDER_EVENTS_TOPIC);
		return newConsumer;
	}

	private static void setId(Order order, Long id) {
		try {
			Field idField = Order.class.getDeclaredField("id");
			idField.setAccessible(true);
			idField.set(order, id);
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException(e);
		}
	}
}
