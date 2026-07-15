# 스키마 생성 전략 결정 — Flyway vs ddl-auto (E1-3 태스크 1)

- **상태**: 확정 (Plan → Generate 반영 완료. 실제 마이그레이션 SQL 작성은 다음 태스크
  "schema 정의/마이그레이션 작성"에서 진행)
- **범위**: 스키마 생성 방식의 결정과 근거, 프로파일별 적용 방식, 후속 태스크 전제 조건만 다룸.
  마이그레이션 SQL/엔티티/시드/`JpaAuditingConfig`는 이 문서의 범위 밖.

## 1. 요구사항·제약 재확인

- `docs/db/schema.md`: 7개 테이블, `CHECK` 제약 3건(`point_balances.balance >= 0`,
  `menus.price >= 0`, `order_items.quantity >= 1`), 복합/단일 인덱스 4건
  (`point_histories(user_id, created_at)`, `orders(ordered_at)`, `order_items(menu_id)`,
  `order_items(order_id)`), 모든 테이블 `created_at`/`updated_at` 감사 컬럼.
- `docs/policy.md`: 감사 컬럼(`createdAt`/`updatedAt`)은 JPA Auditing으로 애플리케이션
  레벨에서 관리 — 이는 스키마 생성 전략과 별개(컬럼 자체는 DDL에 존재해야 함).
- `docs/design/jira-backlog.md` / `jira-manual.md`(E1-3): 시드 데이터·초기 스키마 스토리의
  1번째 태스크가 "스키마 생성 전략 결정"이며, 이후 태스크(스키마/마이그레이션 작성 → 시드 →
  JpaAuditingConfig)가 이 결정을 전제로 순서대로 진행됨.
- `jira-manual.md`(E6-1): 다수 인스턴스 무상태 설계 스토리에서 "다중 인스턴스 기동 시나리오
  문서화(docker-compose scale)"가 요구됨 → 동일 DB에 여러 인스턴스가 동시에 붙는 상황에서
  스키마 생성 로직이 안전해야 함.
- 정리된 제약:
  1. `CHECK` 제약·복합 인덱스는 Hibernate `ddl-auto`(자동 생성)로는 스키마 명세와 100%
     동일하게 재현하기 어렵고(버전·매핑 애너테이션에 의존, 검증 곤란), 명세 문서와 실제
     스키마의 불일치를 조용히 허용할 위험이 있음.
  2. dev(MySQL)·test(H2 MySQL 호환 모드) 두 방언을 동시에 만족해야 함.
  3. E6-1에서 다중 인스턴스가 동일 DB에 동시 기동할 수 있어, 여러 인스턴스가 동시에
     DDL(`update`/`create`)을 실행하면 경합·중복 실행 위험이 있음.

## 2. 옵션 비교

| 축 | Flyway | Hibernate `ddl-auto` |
| --- | --- | --- |
| (a) CHECK/인덱스/타입 재현 정확도 | SQL을 직접 작성 → `schema.md` 명세와 1:1 대응 가능, 리뷰 가능 | `@Check`/`@Index` 등 애너테이션에 의존, 버전·구현체별 동작 차이·누락 위험 |
| (b) dev·test 프로파일 일관성 | 동일 마이그레이션 스크립트를 두 방언(MySQL/H2 MySQL 모드)에 적용 → 단일 소스 | Hibernate가 프로파일별로 각각 스키마를 "추론"해 생성 → 미묘한 차이 가능성 |
| (c) 다중 인스턴스 동시 기동 안전성 | `flyway_schema_history` 테이블 기반 락으로 동시 마이그레이션 실행을 직렬화 | 여러 인스턴스가 동시에 DDL 실행 시 경합/부분 실패 위험, 운영 환경 비권장(공식 문서상 `update` 프로덕션 사용 자제 권고) |
| (d) 변경 이력·롤백 | 버전 파일(`V{n}__description.sql`)로 이력이 코드로 남음, 리뷰·재현 용이 | 이력 없음, 현재 스키마가 어떻게 만들어졌는지 추적 불가 |
| (e) 과제 성격(반복 개발 속도) | 초기 설정 비용(의존성+파일 규칙) 있으나 이후 안정적 | 초기 반복은 빠르지만 제약 재현·다중 인스턴스 안전성에서 결국 한계 |

## 3. 결정

- **Flyway 채택** (Hibernate `ddl-auto`는 `validate`로 전환).
  - 근거: 위 (a)(c)(d)가 이 프로젝트의 핵심 제약(명세된 CHECK/인덱스, E6-1 다중 인스턴스)과
    직접 충돌하는 지점이며, Flyway가 이를 명확히 해결. (e)의 초기 비용은 감내 가능한 수준.
  - 기각한 대안: `ddl-auto`(`update`/`create-drop` 등) 단독 사용은 기각. CHECK 제약·복합
    인덱스를 명세대로 보장할 수 없고, 다중 인스턴스 동시 기동 시 DDL 경합 위험이 남아
    E6-1 요구사항과 상충하기 때문.
- **프로파일별 적용 방식**: dev(MySQL)·test(H2, MySQL 호환 모드) **동일하게 Flyway가 DDL을
  소유**하고, Hibernate `ddl-auto: validate`로 엔티티-스키마 매핑 불일치만 검증(Hibernate는
  스키마를 변경하지 않음). 두 프로파일에 서로 다른 전략을 적용하면 테스트가 실제 배포 스키마를
  검증하지 못하게 되므로, 일관성을 위해 전략을 통일함.

## 4. 후속 태스크(스키마/마이그레이션 작성) 전제 조건

- 마이그레이션 파일 위치: `src/main/resources/db/migration/`
  (Gradle 테스트 클래스패스에 `main` 리소스가 포함되므로 별도 test 전용 마이그레이션 불필요 —
  동일 스크립트가 dev/test 양쪽에 적용됨).
- 네이밍 규칙: `V{순번}__{snake_case 설명}.sql` (예: `V1__init_schema.sql`). 순번은 정수,
  한 번 배포된 버전 파일은 수정하지 않고 새 버전을 추가.
- SQL은 MySQL과 H2(MySQL 호환 모드)에서 모두 동작하는 표준적인 문법을 우선 사용하고,
  MySQL 전용 문법이 필요하면 호환성을 마이그레이션 작성 태스크에서 별도 검증.
- `docs/db/schema.md`의 CHECK 제약·인덱스·타입을 그대로 DDL에 반영(임의 변경 금지, 변경 시
  `docs/db/schema.md`를 먼저 갱신).

## 5. 이번 태스크에서 반영한 변경 사항

- `build.gradle`: `org.flywaydb:flyway-core`(implementation), `org.flywaydb:flyway-mysql`
  (runtimeOnly) 의존성 추가. 마이그레이션 SQL 파일은 아직 없음(다음 태스크).
- `application-dev.yml` / `application-test.yml`: `hibernate.ddl-auto`를 `validate`로 확정,
  주석을 이 문서로 연결.

## 참조

- `docs/db/schema.md`, `docs/policy.md`, `docs/design/jira-backlog.md`,
  `docs/design/jira-manual.md`(E1-3, E6-1), `docs/design/README.md`
