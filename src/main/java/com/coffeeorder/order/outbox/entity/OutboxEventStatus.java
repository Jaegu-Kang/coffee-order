package com.coffeeorder.order.outbox.entity;

/**
 * {@link OutboxEvent#getStatus()}에 대응하는 아웃박스 이벤트 발행 상태.
 * {@code docs/db/schema.md}의 {@code outbox_events.status}(VARCHAR(20)) 컬럼에
 * {@code @Enumerated(EnumType.STRING)}으로 매핑되어 문자열로 저장된다.
 * <p>
 * {@link #PENDING}: 아직 발행되지 않아 릴레이가 폴링·재시도 대상으로 삼는 상태(초기값).
 * {@link #SENT}: {@code OutboxRelay}가 Kafka 발행에 성공한 상태(더 이상 폴링 대상 아님).
 * {@link #FAILED}: 스키마상 예약된 종결 실패 상태. 이번 SCRUM-78 구현은 최대 재시도 횟수·수동
 * 개입 정책을 범위로 다루지 않으므로 {@code OutboxRelay}가 이 상태로 자동 전이시키지 않는다
 * (발행 실패 시에는 {@link #PENDING}을 유지한 채 {@code retryCount}만 증가시켜 다음 폴링에서
 * 계속 재시도한다). 후속 확장(최대 재시도 초과 시 운영자 개입 등)을 위해 값만 정의해 둔다.
 */
public enum OutboxEventStatus {
	PENDING,
	SENT,
	FAILED
}
