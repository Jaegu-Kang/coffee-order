package com.coffeeorder.order.outbox.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import com.coffeeorder.order.outbox.entity.OutboxEvent;
import com.coffeeorder.order.outbox.entity.OutboxEventStatus;

/**
 * {@link OutboxEvent} 영속성 리포지토리(E6-3 태스크4, SCRUM-78). {@code OutboxRelay}가
 * {@link #findByStatusOrderByCreatedAtAsc(OutboxEventStatus, Pageable)}로 {@code status}(주로
 * {@link OutboxEventStatus#PENDING})를 오래된 순으로 배치 조회해 폴링한다(스키마의
 * {@code idx_outbox_events_status_created_at} 인덱스를 그대로 활용).
 */
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

	List<OutboxEvent> findByStatusOrderByCreatedAtAsc(OutboxEventStatus status, Pageable pageable);
}
