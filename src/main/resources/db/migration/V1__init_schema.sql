-- V1__init_schema.sql
-- 근거: docs/db/schema.md (소스 오브 트루스), docs/design/schema-strategy.md (마이그레이션 전략)
-- MySQL 8+ / H2(MySQL 호환 모드) 양쪽에서 동작하는 표준 문법만 사용.

CREATE TABLE users (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	created_at DATETIME(6) NOT NULL,
	updated_at DATETIME(6) NOT NULL
);

CREATE TABLE point_balances (
	user_id BIGINT PRIMARY KEY,
	balance BIGINT NOT NULL DEFAULT 0,
	version BIGINT NOT NULL DEFAULT 0,
	updated_at DATETIME(6) NOT NULL,
	CONSTRAINT fk_point_balances_user FOREIGN KEY (user_id) REFERENCES users (id),
	CONSTRAINT ck_point_balances_balance CHECK (balance >= 0)
);

CREATE TABLE point_histories (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	user_id BIGINT NOT NULL,
	type VARCHAR(10) NOT NULL,
	amount BIGINT NOT NULL,
	balance_after BIGINT NOT NULL,
	created_at DATETIME(6) NOT NULL,
	CONSTRAINT fk_point_histories_user FOREIGN KEY (user_id) REFERENCES users (id)
);

CREATE INDEX idx_point_histories_user_id_created_at ON point_histories (user_id, created_at);

CREATE TABLE menus (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	price BIGINT NOT NULL,
	created_at DATETIME(6) NOT NULL,
	updated_at DATETIME(6) NOT NULL,
	CONSTRAINT ck_menus_price CHECK (price >= 0)
);

CREATE TABLE orders (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	user_id BIGINT NOT NULL,
	total_amount BIGINT NOT NULL,
	status VARCHAR(20) NOT NULL,
	ordered_at DATETIME(6) NOT NULL,
	created_at DATETIME(6) NOT NULL,
	CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users (id)
);

CREATE INDEX idx_orders_ordered_at ON orders (ordered_at);

CREATE TABLE order_items (
	id BIGINT AUTO_INCREMENT PRIMARY KEY,
	order_id BIGINT NOT NULL,
	menu_id BIGINT NOT NULL,
	menu_name VARCHAR(100) NOT NULL,
	unit_price BIGINT NOT NULL,
	quantity INT NOT NULL DEFAULT 1,
	CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders (id),
	CONSTRAINT fk_order_items_menu FOREIGN KEY (menu_id) REFERENCES menus (id),
	CONSTRAINT ck_order_items_quantity CHECK (quantity >= 1)
);

CREATE INDEX idx_order_items_menu_id ON order_items (menu_id);

CREATE INDEX idx_order_items_order_id ON order_items (order_id);
