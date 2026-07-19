# 세션/로컬 캐시 사용처 점검 및 제거 — 다수 인스턴스 무상태 설계 (E6-1 태스크 1)

- **상태**: 확정 (Plan → Generate 반영 완료. 점검 결과 실제 제거 대상 코드가 발견되지 않아
  코드 변경 없이 점검 근거만 문서화함)
- **범위**: `HttpSession`/`@SessionAttributes` 등 서버 세션 사용 여부와, 애플리케이션
  인스턴스 내부에 상태를 들고 있는 로컬 캐시(어노테이션 기반 캐시, 캐시 라이브러리,
  정적/인스턴스 필드에 요청 간 공유되는 가변 컬렉션, `ThreadLocal`)의 점검·제거만 다룬다.
  "잔액=MySQL, 락=Redis, 스트림=Kafka" 같은 상태 외부화 여부 확인, docker-compose scale
  기반 다중 인스턴스 기동 시나리오 문서화는 이 문서의 범위 밖(후속 태스크, 아래 5절 참고).

## 1. 요구사항·제약 재확인

- `docs/design/jira-manual.md`(212~223행, Story E6-1 "다수 인스턴스 무상태 설계"):
  수용 기준은 "인스턴스 증설이 기능에 영향 없음을 근거로 설명·시연"이며, 태스크 1이
  "세션/로컬 캐시 사용처 점검 및 제거".
- `docs/design/jira-backlog.md`(E6-①): 동일 스토리/태스크 정의.
- 전제: 인스턴스가 요청 간 세션이나 인메모리 상태를 들고 있으면, 로드밸런서 뒤에 여러
  인스턴스를 띄웠을 때 요청이 다른 인스턴스로 라우팅되는 순간 상태가 유실되거나
  인스턴스별로 값이 달라질 수 있다. 이를 사전에 점검해 제거(또는 이미 외부화된 저장소로
  대체)하는 것이 이번 태스크의 목적.

## 2. 점검 방법

대상 경로: `src/main/java` 전체(`config/`, `menu/`, `order/`, `point/`, `user/`, `common/`
포함), `src/main/resources/application*.yml`, `src/test/resources/application-test.yml`,
`build.gradle`.

검색 패턴(재현 가능하도록 실행한 명령 기준):

```bash
# 2-1. 세션 사용처
grep -rn "HttpSession\|@SessionAttributes\|@SessionScope\|EnableRedisHttpSession" src/main/java
grep -rn "session" src/main/resources/application*.yml src/test/resources/application*.yml
grep -n "spring-session" build.gradle

# 2-2. 캐시 어노테이션 / 캐시 라이브러리
grep -rn "@Cacheable\|@CacheEvict\|@CachePut\|@EnableCaching\|EhCache\|Caffeine\|guava" \
  src/main/java build.gradle

# 2-3. 인스턴스 간 공유될 수 있는 가변 상태
grep -rn "ConcurrentHashMap\|ThreadLocal" src/main/java
grep -rn "@Scope" src/main/java
grep -rn "static" src/main/java --include="*.java" \
  | grep -v "public static void main" | grep -v "static final" | grep -v "import static"
```

## 3. 점검 결과

### 3-1. 세션 사용처 — 없음

- `HttpSession`, `@SessionAttributes`, `@SessionScope`, `@EnableRedisHttpSession` 사용
  코드가 `src/main/java` 전체에 존재하지 않음.
- `application.yml` / `application-dev.yml` / `application-test.yml`에
  `server.servlet.session` 등 세션 관련 설정이 없음.
- `build.gradle`에 `spring-session*` 계열 의존성이 없음.
- 인증/사용자 식별은 세션이 아닌 요청 파라미터·경로 변수 기반으로 처리되고 있어(예:
  `userId` 파라미터), 서버 세션에 의존하는 구조 자체가 없음.

### 3-2. 로컬(인스턴스 내) 캐시·가변 상태 — 없음

- `@Cacheable`/`@CacheEvict`/`@CachePut`/`@EnableCaching` 등 캐싱 애너테이션이 코드에
  없고, `build.gradle`에도 `spring-boot-starter-cache`, Caffeine, Guava Cache, EhCache 등
  캐시 라이브러리 의존성이 없음.
- `ConcurrentHashMap`, `ThreadLocal` 사용 코드 없음.
- `@Scope` 커스텀 지정(예: `session`, prototype 외 임의 스코프) 사용 없음 — 모든 빈이
  Spring 기본 `singleton` 스코프이되, 아래와 같이 인스턴스 필드는 생성자 주입된
  `private final` 협력 객체(리포지토리 등)뿐이라 요청 간 가변 공유 상태가 없음.
- `static` 키워드 사용처는 전부 `static final` 상수 또는 `DtoClass.from(...)`류의 정적
  팩터리 메서드/유틸 메서드이며, 값을 보관하는 비-final `static` 필드는 없음(위 3-2 검색
  명령 결과 0건).
- 인스턴스(`@Component`/`@Service`) 필드 중 요청 간 공유되는 `Map`/`List`/`Set`/카운터도
  없음. 다만 `HashMap`을 사용하는 두 지점이 있어 오인하지 않도록 명시적으로 확인함:
  - `com.coffeeorder.config.KafkaProducerConfig#kafkaAdmin()` / `#producerFactory()`:
    `Map<String, Object> configs = new HashMap<>();` — 각 `@Bean` 팩터리 메서드가 호출될
    때(스프링 컨텍스트 초기화 시 1회) 지역 변수로 생성되어 Kafka 클라이언트 설정 객체를
    조립하는 데만 쓰이고, 인스턴스 필드에 저장되거나 요청 간 재사용되지 않음.
  - `com.coffeeorder.menu.service.PopularMenuService#getPopularMenus()`:
    `Map<Long, Menu> menusById = new HashMap<>();` — 메서드가 호출될 때마다(요청마다) 새로
    생성되는 지역 변수로, 해당 호출 안에서 조회된 메뉴를 id로 잠깐 인덱싱하는 용도일 뿐
    다음 호출까지 유지되는 캐시가 아님.
  - 두 사례 모두 "인스턴스 상태를 들고 있는 로컬 캐시"가 아니라 메서드 로컬 변수이므로
    제거 대상이 아니며, 계획(Step 2 사전 조사)에서 명시한 대로 리팩터링하지 않음.

## 4. 결론

- 세션(서버 세션, 세션 스코프 빈 등)과 로컬(인스턴스 내부) 캐시·가변 공유 상태 모두
  "없음"으로 확정. 제거할 코드가 존재하지 않아 이번 태스크에서 코드 변경은 발생하지 않음.
- 근거: 2절의 검색 명령을 각각 실행했고, 세션·캐시·`ConcurrentHashMap`·`ThreadLocal`·
  커스텀 `@Scope`·비-final `static` 필드 어느 것도 매치되지 않았음(3절). 유일하게 매치된
  `HashMap` 2건은 메서드 로컬 변수임을 코드로 확인함(3-2절).
- 따라서 이 애플리케이션은 (세션/로컬 캐시 관점에서) 여러 인스턴스로 수평 확장해도 인스턴스
  간 상태 불일치나 세션 유실이 발생하지 않는 무상태 구조를 이미 만족한다.

## 5. 후속 태스크와의 범위 경계

- **상태 외부화 확인**(E6-1 태스크 2): 잔액=MySQL, 락=Redis, 스트림=Kafka처럼 "상태를
  들고 있어야 하는 부분"이 실제로 외부 저장소에 있는지 확인하는 태스크. 이번 문서는 그
  반대 방향인 "인스턴스가 상태를 로컬로 들고 있지 않은지"만 확인했으며, Redis 등 신규
  외부 저장소 도입/검증은 다루지 않는다(이번 작업에서 Redis 관련 코드를 추가하지 않음).
- **다중 인스턴스 기동 시나리오 문서화**(E6-1 태스크 3): `docker-compose scale` 등으로
  실제 다중 인스턴스를 띄워 시연하는 태스크. 이번 문서는 코드/설정 레벨의 정적 점검까지만
  다루고, 실제 다중 인스턴스 기동·시연은 다루지 않는다.

## 참조

- `docs/design/jira-manual.md`(212~223행, E6-1), `docs/design/jira-backlog.md`(E6-①),
  `docs/design/schema-strategy.md`(문서 형식 참고), `docs/design/README.md`,
  `docs/code-convention.md`
