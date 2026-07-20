package com.coffeeorder.order.outbox.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

/**
 * Transactional Outbox 이벤트 엔티티(E6-3 태스크4, SCRUM-78 "(확장) Transactional Outbox 테이블 +
 * 릴레이 설계/구현"). {@code docs/db/schema.md}의 {@code outbox_events} 테이블에 매핑된다.
 * <p>
 * {@code OrderService.order()}가 포인트 차감·주문/주문항목 저장과 같은 물리 트랜잭션 안에서 이
 * 엔티티를 저장해, 저장 자체가 실패(롤백)하면 아웃박스 행도 함께 롤백되고 커밋에 성공하면
 * {@link OutboxEventStatus#PENDING} 행이 함께 커밋되도록 한다(별도 보상 로직 불필요). 실제 Kafka
 * 발행은 이 엔티티를 폴링하는 {@code com.coffeeorder.order.outbox.OutboxRelay}가 트랜잭션 밖에서
 * 별도로 수행한다.
 * <p>
 * {@code aggregateType}/{@code aggregateId}/{@code topic}/{@code payload}는 특정 이벤트
 * 타입({@code OrderEvent})에 결합되지 않은 범용 필드로 설계했다(향후 다른 애그리게잇의 이벤트도
 * 동일 테이블·릴레이를 재사용할 수 있도록). {@code payload}는 실제 발행할 메시지 본문을 JSON
 * 문자열로 직렬화해 저장한다(직렬화 규약은 {@code com.coffeeorder.config.KafkaProducerConfig#orderEventObjectMapper()}
 * 재사용, {@code KafkaTemplate}의 {@code JsonSerializer} 설정과 동일하게 맞춰 발행 시점에 다시
 * {@code Map}으로 역직렬화해도 필드 계약이 유지되도록 한다).
 */
@Entity
@Table(name = "outbox_events")
@EntityListeners(AuditingEntityListener.class)
public class OutboxEvent {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "aggregate_type", nullable = false, length = 50)
	private String aggregateType;

	@Column(name = "aggregate_id", nullable = false)
	private Long aggregateId;

	@Column(name = "topic", nullable = false, length = 100)
	private String topic;

	@Column(name = "payload", nullable = false, length = 4000)
	private String payload;

	@Enumerated(EnumType.STRING)
	@Column(name = "status", nullable = false, length = 20)
	private OutboxEventStatus status;

	@Column(name = "retry_count", nullable = false)
	private Integer retryCount;

	@CreatedDate
	@Column(name = "created_at", nullable = false, updatable = false)
	private LocalDateTime createdAt;

	@LastModifiedDate
	@Column(name = "updated_at", nullable = false)
	private LocalDateTime updatedAt;

	@Column(name = "sent_at")
	private LocalDateTime sentAt;

	protected OutboxEvent() {
	}

	/**
	 * 새 아웃박스 행을 {@link OutboxEventStatus#PENDING}, {@code retryCount=0}으로 생성한다.
	 */
	public OutboxEvent(String aggregateType, Long aggregateId, String topic, String payload) {
		this.aggregateType = aggregateType;
		this.aggregateId = aggregateId;
		this.topic = topic;
		this.payload = payload;
		this.status = OutboxEventStatus.PENDING;
		this.retryCount = 0;
	}

	/**
	 * Kafka 발행에 성공했음을 기록한다({@link OutboxEventStatus#SENT} 전이, {@code sentAt} 기록).
	 */
	public void markSent(LocalDateTime sentAt) {
		this.status = OutboxEventStatus.SENT;
		this.sentAt = sentAt;
	}

	/**
	 * Kafka 발행 시도가 실패했음을 기록한다. 상태는 {@link OutboxEventStatus#PENDING}으로 유지해
	 * 다음 릴레이 폴링에서 다시 시도 대상이 되게 하고, {@code retryCount}만 증가시킨다(at-least-once,
	 * 최대 재시도 초과 정책은 이 확장의 범위 밖 — {@link OutboxEventStatus} Javadoc 참고).
	 */
	public void recordFailure() {
		this.retryCount = this.retryCount + 1;
	}

	public Long getId() {
		return id;
	}

	public String getAggregateType() {
		return aggregateType;
	}

	public Long getAggregateId() {
		return aggregateId;
	}

	public String getTopic() {
		return topic;
	}

	public String getPayload() {
		return payload;
	}

	public OutboxEventStatus getStatus() {
		return status;
	}

	public Integer getRetryCount() {
		return retryCount;
	}

	public LocalDateTime getCreatedAt() {
		return createdAt;
	}

	public LocalDateTime getUpdatedAt() {
		return updatedAt;
	}

	public LocalDateTime getSentAt() {
		return sentAt;
	}
}
