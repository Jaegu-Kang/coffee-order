package com.coffeeorder.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * {@code @Scheduled} 메서드(현재는 {@code com.coffeeorder.order.outbox.OutboxRelay#relayPendingEvents()}
 * 하나) 처리 인프라를 활성화하는 설정(E6-3 태스크4, SCRUM-78 "(확장) Transactional Outbox 테이블 +
 * 릴레이 설계/구현"). 메인 애플리케이션 클래스가 아닌 별도 {@code @Configuration}으로 분리해 웹
 * 슬라이스 테스트가 영향받지 않도록 한다(docs/code-convention.md, {@link JpaAuditingConfig} 선례).
 * <p>
 * {@code test} 프로파일에는 실제 Kafka 브로커가 없으므로, 스케줄러가 백그라운드에서 자동으로
 * {@code OutboxRelay#relayPendingEvents()}를 반복 호출하게 두면 테스트마다 불필요한 Kafka 연결
 * 시도·타이밍 경합이 생길 수 있다. 이를 피하기 위해 이 클래스 자체를 {@code test} 프로파일에서
 * 비활성화해({@code @Profile("!test")}) {@link EnableScheduling}이 등록하는
 * {@code ScheduledAnnotationBeanPostProcessor}가 컨텍스트에 존재하지 않게 한다({@link KafkaProducerConfig}의
 * {@code KafkaAdmin}/{@code NewTopic} 빈을 {@code test} 프로파일에서 비활성화하는 것과 동일한 패턴).
 * {@code OutboxRelay} 빈 자체는 프로파일 제한 없이 항상 등록되므로, 테스트에서는
 * {@code relayPendingEvents()}를 직접 호출해 결정론적으로 검증한다(자동 트리거만 꺼질 뿐 수동 호출은
 * 가능).
 */
@Configuration
@EnableScheduling
@Profile("!test")
public class SchedulingConfig {
}
