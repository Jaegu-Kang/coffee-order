-- V2__seed_menu_user.sql
-- 근거: docs/db/schema.md (users/menus 컬럼 정의), docs/api/menu.md (메뉴 예시값과 일치)
-- 회원가입/메뉴 등록 API는 범위 밖이므로 시드 데이터로 최소 동작 데이터를 제공.
-- MySQL 8+ / H2(MySQL 호환 모드) 양쪽에서 동작하는 표준 문법만 사용.

INSERT INTO users (id, name, created_at, updated_at) VALUES
	(1, '김민준', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)),
	(2, '이서연', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)),
	(3, '박도윤', CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));

INSERT INTO menus (id, name, price, created_at, updated_at) VALUES
	(1, '아메리카노', 3000, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)),
	(2, '카페라떼', 3500, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)),
	(3, '바닐라라떼', 4000, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6));
