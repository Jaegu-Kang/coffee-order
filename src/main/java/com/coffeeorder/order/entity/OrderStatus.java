package com.coffeeorder.order.entity;

/**
 * 주문 상태. {@code docs/db/schema.md}의 {@code orders.status}(VARCHAR(20))에 매핑된다.
 * 현재는 "결제 성공만 저장"하는 범위이므로 {@link #PAID}만 정의한다.
 * 스키마 주석에 따라 향후 {@code CANCELED} 등으로 확장될 수 있으나, 해당 확장은 별도 티켓 범위다.
 */
public enum OrderStatus {
	PAID
}
