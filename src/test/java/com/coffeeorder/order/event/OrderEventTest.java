package com.coffeeorder.order.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.JacksonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.coffeeorder.order.entity.Order;
import com.coffeeorder.order.entity.OrderItem;
import com.coffeeorder.order.entity.OrderStatus;

/**
 * {@link OrderEvent}가 docs/api/order.md의 Kafka 이벤트 계약(orderId,userId,menuId,amount,
 * orderedAt)과 필드/직렬화 형식이 일치하는지 검증하는 순수 단위 테스트(브로커/스프링 컨텍스트 불필요,
 * KafkaProducerConfigTest 선례 참고). 실제 발행(kafkaTemplate.send)은 이 테스트의 범위가 아니다.
 * <p>
 * 직렬화 검증에는 spring-kafka {@code JsonSerializer}가 내부적으로 사용하는
 * {@link JacksonUtils#enhancedObjectMapper()}를 베이스로 재사용하되, {@code orderedAt}이
 * docs/api/order.md 예시처럼 {@code "...Z"} ISO-8601 문자열로 나오도록
 * {@code SerializationFeature.WRITE_DATES_AS_TIMESTAMPS}를 추가로 비활성화한다(세 번째
 * 테스트 메서드의 주석 참고 — {@code enhancedObjectMapper()} 기본값만으로는 이 계약을 만족하지
 * 못하므로, 다음 서브태스크의 실제 발행 설정에도 동일한 커스터마이즈가 필요함을 함께 남긴다).
 */
class OrderEventTest {

	private static final Long ORDER_ID = 1001L;

	private static final Long USER_ID = 1L;

	private static final Long MENU_ID = 2L;

	private static final Long AMOUNT = 3500L;

	private static final Instant ORDERED_AT = Instant.parse("2026-07-13T10:00:00Z");

	@Test
	void 생성자로_만든_OrderEvent의_getter가_입력값과_일치한다() {
		OrderEvent orderEvent = new OrderEvent(ORDER_ID, USER_ID, MENU_ID, AMOUNT, ORDERED_AT);

		assertThat(orderEvent.getOrderId()).isEqualTo(ORDER_ID);
		assertThat(orderEvent.getUserId()).isEqualTo(USER_ID);
		assertThat(orderEvent.getMenuId()).isEqualTo(MENU_ID);
		assertThat(orderEvent.getAmount()).isEqualTo(AMOUNT);
		assertThat(orderEvent.getOrderedAt()).isEqualTo(ORDERED_AT);
	}

	@Test
	void from으로_Order와_OrderItem에서_변환하면_UTC_기준_Instant로_orderedAt이_설정된다() {
		LocalDateTime orderedAtUtcNaive = LocalDateTime.of(2026, 7, 13, 10, 0, 0);
		Order order = new Order(USER_ID, AMOUNT, OrderStatus.PAID, orderedAtUtcNaive);
		setId(order, ORDER_ID);
		OrderItem orderItem = new OrderItem(ORDER_ID, MENU_ID, "카페라떼", AMOUNT, 1);

		OrderEvent orderEvent = OrderEvent.from(order, orderItem);

		assertThat(orderEvent.getOrderId()).isEqualTo(ORDER_ID);
		assertThat(orderEvent.getUserId()).isEqualTo(USER_ID);
		assertThat(orderEvent.getMenuId()).isEqualTo(MENU_ID);
		assertThat(orderEvent.getAmount()).isEqualTo(AMOUNT);
		assertThat(orderEvent.getOrderedAt()).isEqualTo(ORDERED_AT);
	}

	@Test
	void JSON_직렬화_결과가_API_명세_예시와_동일한_키와_UTC_타임스탬프_형식을_가진다() throws Exception {
		OrderEvent orderEvent = new OrderEvent(ORDER_ID, USER_ID, MENU_ID, AMOUNT, ORDERED_AT);
		// JacksonUtils.enhancedObjectMapper()는 spring-kafka JsonSerializer 기본 생성자가 그대로
		// 사용하는 ObjectMapper이지만(JavaTimeModule 등은 자동 등록), WRITE_DATES_AS_TIMESTAMPS는
		// 끄지 않는다(Jackson 기본값 true). 그 상태로는 orderedAt이 "...Z" 문자열이 아니라 epoch 숫자로
		// 직렬화되어 docs/api/order.md 계약과 어긋난다. 이는 다음 서브태스크(발행 로직)에서
		// KafkaTemplate에 커스텀 ObjectMapper를 가진 JsonSerializer를 명시적으로 구성해야 함을 뜻하며,
		// 여기서는 그 요구되는 최종 직렬화 설정(JacksonUtils 베이스 + WRITE_DATES_AS_TIMESTAMPS 비활성)을
		// 재현해 스키마 계약을 검증한다.
		ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper()
				.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

		String json = objectMapper.writeValueAsString(orderEvent);
		Map<String, Object> asMap = objectMapper.readValue(json, Map.class);

		assertThat(asMap.keySet()).containsExactlyInAnyOrder("orderId", "userId", "menuId", "amount", "orderedAt");
		assertThat(asMap.get("orderId")).isEqualTo(ORDER_ID.intValue());
		assertThat(asMap.get("userId")).isEqualTo(USER_ID.intValue());
		assertThat(asMap.get("menuId")).isEqualTo(MENU_ID.intValue());
		assertThat(asMap.get("amount")).isEqualTo(AMOUNT.intValue());
		assertThat(asMap.get("orderedAt")).isEqualTo("2026-07-13T10:00:00Z");
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
