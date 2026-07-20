package com.coffeeorder.point.repository;

import java.util.Optional;

import jakarta.persistence.LockModeType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.coffeeorder.point.entity.PointBalance;

/**
 * {@link PointBalance} 영속성 리포지토리. 현재 요구되는 조회는 PK({@code user_id}) 조회뿐이므로
 * 단순 조회에는 기본 {@code JpaRepository}의 {@code findById}로 충분하다.
 * <p>
 * 충전/차감/주문 시 잔액을 갱신하는 경로처럼 동시성 제어가 필요한 경우에는 {@link #findByIdForUpdate(Long)}로
 * 비관적 락({@code SELECT ... FOR UPDATE})을 건 뒤 조회해야 한다(docs/db/schema.md의 "행 단위 비관적 락"
 * 설계). 다만 이 리포지토리 메서드를 실제로 {@code PointService}/{@code OrderService}의 충전·차감·주문
 * 경로에 적용하는 작업은 이번 서브태스크(SCRUM-72) 범위가 아니라 후속 서브태스크(SCRUM-21의 "④ 충전·차감·
 * 주문 경로 적용")에서 다룬다.
 */
public interface PointBalanceRepository extends JpaRepository<PointBalance, Long> {

	/**
	 * {@code user_id} 기준으로 {@link PointBalance}를 비관적 쓰기 락({@code PESSIMISTIC_WRITE},
	 * {@code SELECT ... FOR UPDATE})으로 조회한다. 락은 트랜잭션이 유지되는 동안에만 유효하므로 호출자는
	 * 반드시 쓰기 트랜잭션({@code @Transactional}) 안에서 이 메서드를 호출해야 한다. 트랜잭션 밖에서
	 * 호출하면 락이 즉시 해제되어 동시성 제어 효과가 없다.
	 *
	 * @param userId 조회할 잔액의 사용자 ID(PK)
	 * @return 잔액이 존재하면 락이 걸린 {@link PointBalance}, 없으면 {@link Optional#empty()}
	 */
	@Lock(LockModeType.PESSIMISTIC_WRITE)
	@Query("select pb from PointBalance pb where pb.userId = :userId")
	Optional<PointBalance> findByIdForUpdate(@Param("userId") Long userId);
}
