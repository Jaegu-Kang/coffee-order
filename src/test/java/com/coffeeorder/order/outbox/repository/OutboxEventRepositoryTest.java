package com.coffeeorder.order.outbox.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;

import com.coffeeorder.config.JpaAuditingConfig;
import com.coffeeorder.order.outbox.entity.OutboxEvent;
import com.coffeeorder.order.outbox.entity.OutboxEventStatus;

/**
 * {@link OutboxEvent} 엔티티와 {@link OutboxEventRepository}의 매핑·폴링 쿼리를 검증하는
 * {@code @DataJpaTest} 슬라이스 테스트(E6-3 태스크4, SCRUM-78). {@code OrderRepositoryTest} 패턴을
 * 재사용한다.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
@ActiveProfiles("test")
class OutboxEventRepositoryTest {

	private static final String PAYLOAD = "{\"orderId\":1001,\"userId\":1,\"menuId\":2,\"amount\":3500,"
			+ "\"orderedAt\":\"2026-07-13T10:00:00Z\"}";

	@Autowired
	private OutboxEventRepository outboxEventRepository;

	@Test
	void 신규_아웃박스_이벤트_저장_시_PENDING_상태와_기본값이_설정되고_필드가_그대로_조회된다() {
		LocalDateTime beforeSave = LocalDateTime.now(ZoneOffset.UTC);

		OutboxEvent saved = outboxEventRepository.saveAndFlush(
				new OutboxEvent("ORDER", 1001L, "order-events", PAYLOAD));

		LocalDateTime afterSave = LocalDateTime.now(ZoneOffset.UTC);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getAggregateType()).isEqualTo("ORDER");
		assertThat(saved.getAggregateId()).isEqualTo(1001L);
		assertThat(saved.getTopic()).isEqualTo("order-events");
		assertThat(saved.getPayload()).isEqualTo(PAYLOAD);
		assertThat(saved.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
		assertThat(saved.getRetryCount()).isZero();
		assertThat(saved.getSentAt()).isNull();
		assertThat(saved.getCreatedAt()).isBetween(
				beforeSave.minus(5, ChronoUnit.SECONDS),
				afterSave.plus(5, ChronoUnit.SECONDS));
		assertThat(saved.getUpdatedAt()).isNotNull();

		OutboxEvent found = outboxEventRepository.findById(saved.getId()).orElseThrow();
		assertThat(found.getAggregateId()).isEqualTo(1001L);
		assertThat(found.getPayload()).isEqualTo(PAYLOAD);
	}

	@Test
	void findByStatusOrderByCreatedAtAsc는_지정한_상태의_행만_오래된_순으로_배치_조회한다() throws InterruptedException {
		OutboxEvent oldest = outboxEventRepository.saveAndFlush(new OutboxEvent("ORDER", 1L, "order-events", PAYLOAD));
		Thread.sleep(10);
		OutboxEvent newest = outboxEventRepository.saveAndFlush(new OutboxEvent("ORDER", 2L, "order-events", PAYLOAD));

		OutboxEvent alreadySent = outboxEventRepository.saveAndFlush(
				new OutboxEvent("ORDER", 3L, "order-events", PAYLOAD));
		alreadySent.markSent(LocalDateTime.now(ZoneOffset.UTC));
		outboxEventRepository.saveAndFlush(alreadySent);

		List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatusOrderByCreatedAtAsc(
				OutboxEventStatus.PENDING, PageRequest.of(0, 10));

		assertThat(pendingEvents).extracting(OutboxEvent::getAggregateId)
				.containsExactly(oldest.getAggregateId(), newest.getAggregateId());
		assertThat(pendingEvents).extracting(OutboxEvent::getStatus)
				.containsOnly(OutboxEventStatus.PENDING);
	}

	@Test
	void recordFailure는_상태를_PENDING으로_유지한_채_retryCount만_증가시킨다() {
		OutboxEvent saved = outboxEventRepository.saveAndFlush(new OutboxEvent("ORDER", 1L, "order-events", PAYLOAD));

		saved.recordFailure();
		OutboxEvent updated = outboxEventRepository.saveAndFlush(saved);

		assertThat(updated.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
		assertThat(updated.getRetryCount()).isEqualTo(1);
	}

	@Test
	void markSent는_상태를_SENT로_전환하고_sentAt을_기록한다() {
		OutboxEvent saved = outboxEventRepository.saveAndFlush(new OutboxEvent("ORDER", 1L, "order-events", PAYLOAD));
		LocalDateTime sentAt = LocalDateTime.now(ZoneOffset.UTC);

		saved.markSent(sentAt);
		OutboxEvent updated = outboxEventRepository.saveAndFlush(saved);

		assertThat(updated.getStatus()).isEqualTo(OutboxEventStatus.SENT);
		assertThat(updated.getSentAt()).isEqualTo(sentAt);
	}
}
