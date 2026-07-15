# CLAUDE.md — coffee-order 작업 규칙

## 프로젝트
- Spring Boot 4.1 / Java 17 / Gradle (`./gradlew`)
- 루트 패키지: `com.coffeeorder`
- 빌드 `./gradlew build` · 테스트 `./gradlew test` · 컴파일 `./gradlew compileJava`

## 실행 원칙 — Plan → Generate → Evaluate
기능 작업은 계획 → 구현 → 독립 검증 순서로 진행합니다. 계획에 없는 범위 확장(불필요한 리팩터링 등) 금지.

## 규칙 / 참조
- 코드 컨벤션: `docs/code-convention.md`
- API 명세: `docs/api/` · DB 스키마: `docs/db/`
- 설계·백로그: `docs/design/`
- 공유 비즈니스 규칙: `docs/policy.md`
