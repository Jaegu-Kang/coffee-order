package com.coffeeorder.point.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.coffeeorder.point.entity.PointBalance;

/**
 * {@link PointBalance} 영속성 리포지토리. 현재 요구되는 조회는 PK({@code user_id}) 조회뿐이므로
 * 기본 {@code JpaRepository}의 {@code findById}로 충분하다. 충전/차감을 위한 비관적 락 조회 등
 * 커스텀 쿼리는 이 리포지토리의 범위가 아니다(후속 서브태스크에서 다룬다).
 */
public interface PointBalanceRepository extends JpaRepository<PointBalance, Long> {
}
