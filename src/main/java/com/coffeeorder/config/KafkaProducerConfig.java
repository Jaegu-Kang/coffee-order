package com.coffeeorder.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Kafka Producer 인프라 설정(SCRUM-59 / E4-2 태스크1: "KafkaConfig/Producer 설정
 * (KafkaTemplate, order-events 토픽)", docs/design/jira-manual.md 168행).
 * {@code order-events} 토픽 프로비저닝과 {@link KafkaTemplate} 빈 등록까지만 담당하며,
 * OrderEvent 스키마 정의·실제 발행 로직·발행 검증 테스트는 후속 서브태스크(태스크2~4)의
 * 범위다(docs/design/jira-manual.md 169~171행).
 * build.gradle에는 {@code spring-kafka} 라이브러리만 포함되어 있고 Boot의 Kafka 자동 구성
 * 모듈(starter)은 의존성에 없으므로, {@code spring.kafka.bootstrap-servers} 값을 Boot의
 * {@code KafkaProperties}가 아닌 {@code @Value}로 직접 주입한다(application-dev.yml의
 * {@code spring.kafka.bootstrap-servers}, docker-compose.yml kafka 서비스와 동일한 기본값
 * localhost:9092 사용).
 * 메인 애플리케이션 클래스가 아닌 별도 {@code @Configuration}으로 분리해 웹 슬라이스 테스트가
 * 영향받지 않도록 한다(docs/code-convention.md, JpaAuditingConfig 선례).
 * <p>
 * {@code test} 프로파일에는 브로커가 없어 {@link KafkaAdmin}의 토픽 생성 시도 시 내부
 * {@code AdminClient}가 재연결을 반복하며 {@code @SpringBootTest} 컨텍스트 기동/종료가 10초
 * 이상 지연되는 현상이 확인되었다(SCRUM-59 Step 3). {@code operationTimeout}을 낮춰도
 * {@code updateClusterId} 호출 자체만 짧아질 뿐, 실패 후에도 {@code AdminClient}의 백그라운드
 * 재연결 스레드가 JVM 종료 시점까지 남아 있어 지연이 사라지지 않으므로, {@code test} 프로파일에서는
 * {@link #kafkaAdmin()}/{@link #orderEventsTopic()} 빈 자체를 비활성화해 admin 클라이언트가
 * 아예 생성되지 않게 한다. 브로커가 정상 기동된 환경(dev 등)에서는 충분한 3초로 낮춰 유지한다.
 */
@Configuration
public class KafkaProducerConfig {

	/**
	 * 주문 이벤트가 발행될 토픽명(다음 서브태스크의 발행 로직에서 재사용하도록 public으로 노출).
	 */
	public static final String ORDER_EVENTS_TOPIC = "order-events";

	private final String bootstrapServers;

	private final int adminOperationTimeoutSeconds;

	public KafkaProducerConfig(@Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
			@Value("${spring.kafka.admin.operation-timeout-seconds:3}") int adminOperationTimeoutSeconds) {
		this.bootstrapServers = bootstrapServers;
		this.adminOperationTimeoutSeconds = adminOperationTimeoutSeconds;
	}

	/**
	 * {@code order-events} 토픽을 프로비저닝한다. docker-compose.yml의 kafka 서비스가 KRaft
	 * 단일 브로커(offsets replication factor 1)이므로 replicas를 1보다 크게 잡으면 로컬 환경에서
	 * 토픽 생성이 실패한다.
	 * {@code test} 프로파일에는 브로커가 없어 비활성화한다(클래스 상단 설명 참고).
	 */
	@Bean
	@Profile("!test")
	public NewTopic orderEventsTopic() {
		return TopicBuilder.name(ORDER_EVENTS_TOPIC)
				.partitions(1)
				.replicas(1)
				.build();
	}

	/**
	 * {@link NewTopic} 빈을 실제로 프로비저닝하기 위한 Kafka 관리 클라이언트.
	 * Boot의 Kafka 자동 구성 모듈이 의존성에 없어 직접 등록한다.
	 * {@code test} 프로파일에는 브로커가 없어 비활성화한다(클래스 상단 설명 참고).
	 */
	@Bean
	@Profile("!test")
	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		KafkaAdmin admin = new KafkaAdmin(configs);
		admin.setOperationTimeout(adminOperationTimeoutSeconds);
		return admin;
	}

	/**
	 * key는 문자열, value는 향후 OrderEvent 등 임의 객체를 담을 수 있도록 JSON 직렬화한다
	 * (구체 타입은 태스크2에서 확정 예정이라 제네릭은 {@code Object}로 유지).
	 * {@code KEY_SERIALIZER_CLASS_CONFIG}/{@code VALUE_SERIALIZER_CLASS_CONFIG} configs 항목은
	 * (기존 테스트가 검증하므로) 그대로 두되, 실제 사용되는 {@link JsonSerializer} 인스턴스는
	 * {@link JacksonUtils#enhancedObjectMapper()}에 {@code WRITE_DATES_AS_TIMESTAMPS}를 비활성화한
	 * {@link ObjectMapper}로 교체해, {@code orderedAt} 같은 {@link java.time.Instant} 필드가
	 * epoch 숫자가 아닌 docs/api/order.md 예시의 {@code "...Z"} ISO-8601 문자열로 직렬화되게 한다
	 * (OrderEventTest의 직렬화 계약과 동일한 설정, SCRUM-61).
	 */
	@Bean
	public ProducerFactory<String, Object> producerFactory() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		ObjectMapper orderEventObjectMapper = JacksonUtils.enhancedObjectMapper()
				.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		return new DefaultKafkaProducerFactory<>(configs, StringSerializer::new,
				() -> new JsonSerializer<>(orderEventObjectMapper));
	}

	@Bean
	public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}
}
