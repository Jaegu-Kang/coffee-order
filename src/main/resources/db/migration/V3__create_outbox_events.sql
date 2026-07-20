-- V3__create_outbox_events.sql
-- 근거: docs/db/schema.md § outbox_events (소스 오브 트루스), docs/design/schema-strategy.md (마이그레이션 전략)
-- E6-3 태스크4(SCRUM-78, 확장): Transactional Outbox 패턴을 위한 아웃박스 테이블.
-- MySQL 8+ / H2(MySQL 호환 모드) 양쪽에서 동작하는 표준 문법만 사용(V1__init_schema.sql 관례 준수).

CREATE TABLE outbox_events (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	aggregate_type VARCHAR(50) NOT NULL,
	aggregate_id BIGINT NOT NULL,
	topic VARCHAR(100) NOT NULL,
	payload VARCHAR(4000) NOT NULL,
	status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
	retry_count INT NOT NULL DEFAULT 0,
	created_at DATETIME(6) NOT NULL,
	updated_at DATETIME(6) NOT NULL,
	sent_at DATETIME(6)
);

CREATE INDEX idx_outbox_events_status_created_at ON outbox_events (status, created_at);
