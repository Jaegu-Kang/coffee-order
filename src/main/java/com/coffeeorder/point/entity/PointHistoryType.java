package com.coffeeorder.point.entity;

/**
 * {@link PointHistory#getType()}에 대응하는 포인트 이력 유형.
 * {@code docs/db/schema.md}의 {@code point_histories.type}(VARCHAR(10)) 컬럼에
 * {@code @Enumerated(EnumType.STRING)}으로 매핑되어 {@code "CHARGE"}/{@code "USE"} 문자열로 저장된다.
 */
public enum PointHistoryType {
	CHARGE,
	USE
}
