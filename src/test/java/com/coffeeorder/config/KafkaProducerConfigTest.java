package com.coffeeorder.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * {@link KafkaProducerConfig}를 브로커/스프링 컨텍스트 없이 직접 인스턴스화해 검증하는
 * 순수 단위 테스트(JpaAuditingConfigTest와 달리 슬라이스 컨텍스트가 필요 없어 {@code @DataJpaTest}
 * 등을 사용하지 않는다). EmbeddedKafka 기반 실제 발행 검증은 태스크4(발행 검증 테스트) 범위다
 * (docs/design/jira-manual.md 171행).
 */
class KafkaProducerConfigTest {

	private static final String TEST_BOOTSTRAP_SERVERS = "localhost:9092";

	private static final int TEST_ADMIN_OPERATION_TIMEOUT_SECONDS = 3;

	private final KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig(TEST_BOOTSTRAP_SERVERS,
			TEST_ADMIN_OPERATION_TIMEOUT_SECONDS);

	@Test
	void orderEventsTopic_이름과_파티션_복제계수가_docker_compose_단일_브로커와_일치한다() {
		NewTopic orderEventsTopic = kafkaProducerConfig.orderEventsTopic();

		assertThat(orderEventsTopic.name()).isEqualTo(KafkaProducerConfig.ORDER_EVENTS_TOPIC);
		assertThat(orderEventsTopic.numPartitions()).isEqualTo(1);
		assertThat(orderEventsTopic.replicationFactor()).isEqualTo((short) 1);
	}

	@Test
	void kafkaAdmin_설정값에_bootstrap_servers가_반영된다() {
		KafkaAdmin kafkaAdmin = kafkaProducerConfig.kafkaAdmin();

		assertThat(kafkaAdmin.getConfigurationProperties())
				.containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_BOOTSTRAP_SERVERS);
		assertThat(kafkaAdmin.getOperationTimeout()).isEqualTo(TEST_ADMIN_OPERATION_TIMEOUT_SECONDS);
	}

	@Test
	void producerFactory_설정값에_bootstrap_servers와_직렬화_클래스가_반영된다() {
		ProducerFactory<String, Object> producerFactory = kafkaProducerConfig.producerFactory();

		assertThat(producerFactory.getConfigurationProperties())
				.containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_BOOTSTRAP_SERVERS)
				.containsEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
				.containsEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
	}

	@Test
	void kafkaTemplate은_전달받은_ProducerFactory를_그대로_사용한다() {
		ProducerFactory<String, Object> producerFactory = kafkaProducerConfig.producerFactory();

		KafkaTemplate<String, Object> kafkaTemplate = kafkaProducerConfig.kafkaTemplate(producerFactory);

		assertThat(kafkaTemplate.getProducerFactory()).isSameAs(producerFactory);
	}
}
