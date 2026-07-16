package com.coffeeorder.point.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.coffeeorder.point.entity.PointBalance;

/**
 * {@link PointBalance} 영속성 리포지토리. 현재 요구되는 조회는 PK({@code user_id}) 조회뿐이므로
 * 기본 {@code JpaRepository}의 {@code findById}로 충분하다. 충전/차감을 위한 비관적 락 조회 등
 * 커스텀 쿼리는 이 리포지토리의 범위가 아니다(후속 서브태스크에서 다룬다).
 * {@link PointBalance} 영속성 리포지토리. PK가 {@code user_id}이므로 기본
 * {@code findById(userId)}로 잔액 존재 여부를 조회할 수 있다(행이 없으면 잔액 0으로 간주,
 * docs/db/schema.md {@code balance DEFAULT 0}). 비관적 락({@code @Lock(PESSIMISTIC_WRITE)})은
 * 도전 과제(E6) 범위이므로 이번 티켓(SCRUM-55)에서는 추가하지 않는다.
 */
public interface PointBalanceRepository extends JpaRepository<PointBalance, Long> {
}
