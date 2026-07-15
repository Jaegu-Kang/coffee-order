# 코드 컨벤션 (Java / Spring Boot)

이 프로젝트의 모든 Java 코드는 아래 규칙을 따릅니다. 코드 작성/수정 전 반드시 확인하세요.

## 계층 구조

패키지는 기능 단위로 묶고, 계층을 분리합니다.

```
com.myharness.<feature>
├── controller   # HTTP 진입점 (@RestController)
├── service      # 비즈니스 로직 (@Service, @Transactional)
├── repository   # 영속성 (Spring Data 등)
├── entity       # 도메인 엔티티 (JPA 등)
├── dto          # 요청/응답 DTO (엔티티 미노출)
└── exception    # 도메인 예외
```

공통 설정은 `com.myharness.config`, 공통 예외 처리는 `@RestControllerAdvice`.

## 의존성 주입

- **생성자 주입**만 사용한다. 필드는 `private final`.
- `@Autowired` 필드 주입 금지.

## 트랜잭션

- 서비스 클래스 기본값: `@Transactional(readOnly = true)`.
- 쓰기 메서드(create/update/delete)에만 `@Transactional`을 덧붙인다.

## 컨트롤러 / API

- `@RestController` + 리소스 경로는 `/api/<resource>` 복수형.
- 요청 바디는 DTO로 받고 **Bean Validation**(`@NotBlank`, `@Size`, `@NotNull` 등)으로 검증.
- 응답은 **엔티티를 직접 노출하지 않고** Response DTO로 매핑(`from(...)`).
- 생성은 `201 Created` 등 의미에 맞는 상태 코드 사용.

## 예외 / 에러 응답

- `@RestControllerAdvice`로 전역 처리.
- 에러 응답 형식: `{ "code": "...", "message": "..." }`.
- `code`/`message`는 매직 스트링 대신 상수로 관리.
- 검증 실패 → `400 VALIDATION_ERROR`, 도메인 규칙 위반 → 규칙별 코드(예: `INVALID_TIME_RANGE`).

## 스타일

- **탭 들여쓰기**.
- **Lombok 미사용** (getter/생성자 등은 명시적으로 작성).
- 엔티티/DTO 필드명은 camelCase, DB 컬럼은 명세(`docs/db/`)를 따른다.
- 매직 넘버·문자열은 상수화.

## 테스트

- 서비스: Mockito 단위 테스트(`@Mock` repository) — DB 없이 로직 검증.
- 컨트롤러: `@WebMvcTest` 슬라이스(service는 `@MockitoBean`).
  - Spring Boot 4에서 `@WebMvcTest`는 `org.springframework.boot.webmvc.test.autoconfigure` 패키지.
- 통합(`@SpringBootTest`)은 DB 등 인프라가 필요하므로, 가능하면 슬라이스/단위로 대체.
- `@EnableJpaAuditing` 등 JPA 인프라 설정은 **메인 클래스에 직접 붙이지 말고** 별도
  `@Configuration`(예: `config/JpaAuditingConfig`)으로 분리 → 웹 슬라이스 테스트가 깨지지 않음.
