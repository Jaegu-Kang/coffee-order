package com.coffeeorder.order.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * 주문 항목(메뉴 스냅샷) 엔티티. {@code docs/db/schema.md}의 {@code order_items} 테이블에 매핑된다.
 * {@code menuName}/{@code unitPrice}는 주문 시점의 메뉴명·단가를 그대로 복사해 저장하는 스냅샷으로,
 * "메뉴명·단가를 스냅샷으로 저장하는 이유: 이후 메뉴 가격이 바뀌어도 과거 주문 금액·이력이
 * 불변이어야 하기 때문(데이터 일관성)"(docs/db/schema.md § order_items). {@code orderId}/{@code menuId}는
 * {@code Order.userId}와 동일하게 연관관계가 아닌 단순 FK 컬럼으로 둔다. {@code order_items} 테이블에는
 * {@code created_at}/{@code updated_at} 컬럼이 없으므로 이 엔티티에는 감사 필드를 두지 않는다
 * (docs/db/schema.md 확인).
 */
@Entity
@Table(name = "order_items")
public class OrderItem {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "order_id", nullable = false)
	private Long orderId;

	@Column(name = "menu_id", nullable = false)
	private Long menuId;

	@Column(name = "menu_name", nullable = false, length = 100)
	private String menuName;

	@Column(name = "unit_price", nullable = false)
	private Long unitPrice;

	@Column(name = "quantity", nullable = false)
	private Integer quantity;

	protected OrderItem() {
	}

	public OrderItem(Long orderId, Long menuId, String menuName, Long unitPrice, Integer quantity) {
		this.orderId = orderId;
		this.menuId = menuId;
		this.menuName = menuName;
		this.unitPrice = unitPrice;
		this.quantity = quantity;
	}

	public Long getId() {
		return id;
	}

	public Long getOrderId() {
		return orderId;
	}

	public Long getMenuId() {
		return menuId;
	}

	public String getMenuName() {
		return menuName;
	}

	public Long getUnitPrice() {
		return unitPrice;
	}

	public Integer getQuantity() {
		return quantity;
	}
}
