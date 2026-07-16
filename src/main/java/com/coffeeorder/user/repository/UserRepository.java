package com.coffeeorder.user.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.coffeeorder.user.entity.User;

/**
 * {@link User} 영속성 리포지토리. 현재 요구되는 조회는 존재 확인/PK 조회뿐이므로
 * 기본 {@code JpaRepository}의 {@code findById}/{@code existsById}로 충분하다(커스텀 쿼리는 범위 밖).
 */
public interface UserRepository extends JpaRepository<User, Long> {
}
