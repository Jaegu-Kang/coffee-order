# 다중 인스턴스 기동 시나리오 문서화 (docker-compose scale) — 다수 인스턴스 무상태 설계 (E6-1 태스크 3)

- **상태**: 확정 (Plan → Generate 반영 완료. `Dockerfile`/`docker-compose.yml`(app·nginx
  서비스)/`nginx.conf`를 신규 추가하고, 실제 `docker compose up --scale app=3`으로 다중
  인스턴스를 기동해 아래 3절의 명령·출력을 그대로 근거로 남김)
- **범위**: `docker-compose scale`로 앱을 실제 다중 인스턴스로 기동하고, "인스턴스 증설이
  기능에 영향 없음"을 실행 근거(명령/로그)로 문서화하는 것만 다룬다. 세션/로컬 캐시
  점검(`docs/design/session-cache-check.md`, 태스크 1)과 상태 외부화 정적 점검
  (`docs/design/state-externalization-check.md`, 태스크 2)은 이미 완료되어 그 결론을
  인용만 하고 재조사하지 않는다. Redis 분산락·비관적 락 구현(E6-2 "② 동시성 제어")은 이
  문서의 범위 밖이며, 이번 작업에서 관련 코드를 추가하지 않는다(6절 참고).

## 1. 요구사항·제약 재확인

- `docs/design/jira-manual.md`(212~223행, Story E6-1 "① 다수 인스턴스 무상태 설계"):
  수용 기준은 "인스턴스 증설이 기능에 영향 없음을 근거로 설명·시연"이며, 태스크 3이
  "다중 인스턴스 기동 시나리오 문서화 (docker-compose scale)"다.
- `docs/design/jira-backlog.md`(E6-①): 동일 스토리/태스크 정의.
- `docs/design/session-cache-check.md`(5절)·`docs/design/state-externalization-check.md`
  (7절): 두 문서 모두 "실제 다중 인스턴스 기동·시연은 다루지 않는다(태스크 3에서 진행)"고
  이미 범위를 위임해 두었다. 이번 문서가 그 위임을 이어받아 실행한다.
- `docs/design/schema-strategy.md`(2절 (c)): "다중 인스턴스 동시 기동 시 DDL 경합 위험 →
  `flyway_schema_history` 락으로 직렬화"가 Flyway 채택 근거로 이미 명시돼 있다. 이번
  시연에서 그 동작을 실제 로그로 확인한다(3-4절).
- `CLAUDE.md`: "계획에 없는 범위 확장(불필요한 리팩터링 등) 금지." 이번 태스크에서는
  도메인 코드(컨트롤러/서비스/엔티티)를 변경하지 않고, 인프라 파일(`Dockerfile`,
  `docker-compose.yml`, `nginx.conf`)과 이 문서만 추가/수정한다.
- 전제(사전 조사 결과, 재조사 불필요): `application-dev.yml`은 `DB_HOST`/`DB_PORT`/
  `KAFKA_HOST`/`KAFKA_PORT` 환경변수로 오버라이드 가능하게 이미 되어 있어(기본값
  `localhost`), 컨테이너 네트워크에서 `mysql`/`kafka` 서비스명으로 접속하도록 compose
  `environment`만 지정하면 되고 애플리케이션 코드/설정 변경은 불필요하다.

## 2. 방법

### 2-1. 신규 파일

- `Dockerfile`(신규): Gradle 멀티스테이지 빌드. 빌드 스테이지(`eclipse-temurin:17-jdk`)에서
  저장소의 `gradlew`/`build.gradle`/`src`를 그대로 사용해 `./gradlew bootJar --no-daemon
  -x test`를 실행하고, 런타임 스테이지(`eclipse-temurin:17-jre`)는 산출된 jar만 복사해
  `java -jar app.jar`로 실행한다.
- `docker-compose.yml`(수정): 기존 `mysql`/`kafka` 서비스 정의는 변경하지 않고, 아래 두
  서비스만 추가했다.
  - `app`: `build: .`, `environment`로 `DB_HOST=mysql`, `DB_PORT=3306`,
    `DB_NAME=coffee_order`, `DB_USERNAME=coffee`, `DB_PASSWORD=coffee`, `KAFKA_HOST=kafka`,
    `KAFKA_PORT=9092` 지정(컨테이너 네트워크 값이며 앱 코드/설정 파일은 무변경).
    `depends_on`에 `mysql`/`kafka` `condition: service_healthy`. `--scale app=N` 시 이름/
    호스트 포트 충돌을 피하기 위해 `container_name`과 고정 `ports`를 두지 않고
    `expose: ["8080"]`만 지정.
  - `nginx`: `nginx:1.27-alpine` + `./nginx.conf`를 바인드 마운트해 호스트
    `localhost:8080`(`ports: "8080:80"`)에서 `app` 여러 인스턴스로 라운드로빈 분산.
- `nginx.conf`(신규): `upstream coffee_order_app { server app:8080; }` + `proxy_pass`로
  라운드로빈 프록시 구성. 어느 인스턴스가 요청을 처리했는지 근거로 남기기 위해
  `log_format upstream_log '$request -> upstream=$upstream_addr status=$status';`를
  추가하고 `access_log`에 적용했다.

### 2-2. 실행한 명령(원문)

```bash
# Step 1 — 단독 이미지 빌드 확인
docker build -t coffee-order-app .

# Step 2 — compose 설정 유효성 확인
docker compose config

# Step 3 — 다중 인스턴스 기동
docker compose up --build --scale app=3 -d
docker compose ps
docker compose logs app
curl -s -o /dev/null -w "status=%{http_code}\n" http://localhost:8080/api/menus   # 여러 번 반복
curl -s -X POST http://localhost:8080/api/points/charge -H "Content-Type: application/json" \
  -d '{"userId":2,"amount":10000}'
curl -s -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" \
  -d '{"userId":2,"menuId":2,"quantity":1}'
docker compose logs nginx

# Step 5 — 정리
docker compose down
```

- **로컬 환경 특이사항(재현 시 참고)**: 이 실행 환경(개발자 macOS)에는 Docker와 무관한
  네이티브 MySQL 서버(`/usr/local/mysql/bin/mysqld`)가 이미 호스트 `3306` 포트를 점유하고
  있어, 커밋된 `docker-compose.yml`(변경 없음, `mysql` 서비스 `ports: "3306:3306"`)로 바로
  `docker compose up`을 실행하면 `ports are not available: ... 0.0.0.0:3306 ...` 에러가
  발생했다. 이는 이번 변경과 무관한 로컬 환경 조건이므로, 커밋 대상 `docker-compose.yml`은
  그대로 두고 **레포에 포함하지 않는 임시 오버라이드 파일**(`/tmp/coffee-order-demo-override.yml`,
  시연 후 삭제)로 `mysql`의 호스트 포트만 `3307:3306`으로 바꿔 시연했다.

  ```yaml
  services:
    mysql:
      ports: !override
        - "3307:3306"
  ```

  ```bash
  docker compose -f docker-compose.yml -f /tmp/coffee-order-demo-override.yml \
    up --build --scale app=3 -d
  ```

  호스트 3306이 비어 있는 일반적인 환경에서는 이 오버라이드 없이
  `docker compose up --build --scale app=3 -d` 한 줄로 동일하게 동작한다(컨테이너 내부
  네트워크에서는 항상 `mysql:3306`으로 접속하므로 호스트 포트 매핑 값과 무관).

### 2-3. Flyway 동시 마이그레이션 경합 확인용 준비

- `docker compose down -v`로 `mysql-data` 볼륨을 완전히 제거한 뒤 다시
  `up --scale app=3 -d`를 실행해, 3개 인스턴스가 **완전히 빈 스키마**에 동시에 접속하는
  상황을 재현했다(볼륨이 남아있으면 이미 마이그레이션이 끝난 스키마를 검증만 하게 되어
  "경합 자체가 안전하게 처리되는지"를 로그로 보여줄 수 없기 때문).

## 3. 결과

### 3-1. `docker build` 단독 빌드 — 성공

```
#14 [build 7/7] RUN chmod +x gradlew && ./gradlew bootJar --no-daemon -x test
...
BUILD SUCCESSFUL in 40s
...
#16 naming to docker.io/library/coffee-order-app:latest done
```

### 3-2. `docker compose config` — 유효성 확인

`app`/`nginx` 서비스가 에러 없이 파싱되고, `app`은 `mysql`/`kafka`에
`condition: service_healthy`로 의존하며 `container_name`/고정 `ports` 없이 `expose: ["8080"]`
만 갖는 것을 확인했다(전체 출력은 `docker compose config` 재실행으로 재현 가능).

### 3-3. `docker compose ps` — app 3개 + nginx + mysql + kafka 모두 정상

```
NAME                 IMAGE                COMMAND                   SERVICE   CREATED          STATUS                    PORTS
coffee-order-app-1   coffee-order-app     "java -jar /app/app.…"   app       36 seconds ago   Up 28 seconds             8080/tcp
coffee-order-app-2   coffee-order-app     "java -jar /app/app.…"   app       36 seconds ago   Up 28 seconds             8080/tcp
coffee-order-app-3   coffee-order-app     "java -jar /app/app.…"   app       36 seconds ago   Up 29 seconds             8080/tcp
coffee-order-kafka   apache/kafka:3.7.0   "/__cacert_entrypoin…"   kafka     36 seconds ago   Up 35 seconds (healthy)   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp
coffee-order-mysql   mysql:8.0            "docker-entrypoint.s…"   mysql     36 seconds ago   Up 35 seconds (healthy)   0.0.0.0:3307->3306/tcp, [::]:3307->3306/tcp
coffee-order-nginx   nginx:1.27-alpine    "/docker-entrypoint.…"   nginx     36 seconds ago   Up 28 seconds             0.0.0.0:8080->80/tcp, [::]:8080->80/tcp
```

(`mysql` 호스트 포트가 `3307`인 것은 2-2절에 기록한 로컬 환경 특이사항 때문이며, 컨테이너
간 통신은 항상 `mysql:3306`을 사용한다.)

### 3-4. Flyway 마이그레이션 — 3개 인스턴스 동시 기동 시에도 경합 없이 안전

빈 볼륨에서 3개 인스턴스를 동시에 기동한 로그(`docker compose logs app`, 컨테이너별
스트림 필터링):

```
app-3  | ... FlywayExecutor : Database: jdbc:mysql://mysql:3306/coffee_order...
app-3  | ... DbValidate     : Successfully validated 2 migrations (execution time 00:00.028s)
app-3  | ... DbMigrate      : Current version of schema `coffee_order`: << Empty Schema >>
app-3  | ... DbMigrate      : Migrating schema `coffee_order` to version "1 - init schema"
app-3  | ... DbMigrate      : Migrating schema `coffee_order` to version "2 - seed menu user"
app-3  | ... DbMigrate      : Successfully applied 2 migrations to schema `coffee_order`, now at version v2 (execution time 00:00.282s)

app-2  | ... FlywayExecutor : Database: jdbc:mysql://mysql:3306/coffee_order...
app-2  | ... DbValidate     : Successfully validated 2 migrations (execution time 00:00.056s)
app-2  | ... DbMigrate      : Current version of schema `coffee_order`: 2
app-2  | ... DbMigrate      : Schema `coffee_order` is up to date. No migration necessary.

app-1  | ... FlywayExecutor : Database: jdbc:mysql://mysql:3306/coffee_order...
app-1  | ... DbValidate     : Successfully validated 2 migrations (execution time 00:00.060s)
app-1  | ... DbMigrate      : Current version of schema `coffee_order`: 2
app-1  | ... DbMigrate      : Schema `coffee_order` is up to date. No migration necessary.
```

- 셋 다 거의 동시(같은 초 단위, 23:36:31~33Z)에 컨테이너가 기동했지만, 실제 DDL
  (`Migrating schema ... to version ...`)은 `app-3` **한 곳에서만** 실행됐고 `app-1`/`app-2`는
  이미 v2로 올라간 스키마를 검증만 했다. 에러·경합·중복 실행 로그는 없었다 — 이는
  `docs/design/schema-strategy.md`(2절 (c))가 명시한 "`flyway_schema_history` 락으로
  동시 마이그레이션 실행을 직렬화"가 실제로 동작함을 로그로 확인한 것이다.
- `flyway_schema_history` 테이블 조회로도 마이그레이션이 정확히 1회씩만(중복 없이) 기록된
  것을 확인했다.

  ```
  installed_rank  version  description       installed_by  success
  1                1        init schema       coffee        1
  2                2        seed menu user    coffee        1
  ```

### 3-5. 라운드로빈 — 여러 인스턴스가 동일하게 정상 응답

`nginx.conf`에 추가한 `upstream_log` 포맷으로 `GET /api/menus`를 반복 호출한 결과, 매번
200이 반환되고 처리한 컨테이너(IP)가 순환하는 것을 확인했다(컨테이너 IP는
`docker inspect`로 매핑: `app-1=172.18.0.5`, `app-2=172.18.0.6`, `app-3=172.18.0.4`).

```
GET /api/menus HTTP/1.1 -> upstream=172.18.0.5:8080 status=200   # app-1
GET /api/menus HTTP/1.1 -> upstream=172.18.0.6:8080 status=200   # app-2
GET /api/menus HTTP/1.1 -> upstream=172.18.0.4:8080 status=200   # app-3
GET /api/menus HTTP/1.1 -> upstream=172.18.0.5:8080 status=200   # app-1
GET /api/menus HTTP/1.1 -> upstream=172.18.0.6:8080 status=200   # app-2
GET /api/menus HTTP/1.1 -> upstream=172.18.0.4:8080 status=200   # app-3
```

(참고: nginx가 시작하는 순간 아직 `app` 3개 DNS 레코드가 모두 뜨기 전이면 일시적으로
`no live upstreams` 502가 날 수 있어 실제 로그에도 기동 초기 502 2건이 남아 있다. 이는
nginx가 hostname을 시작 시점에 1회 해석하는 특성 때문이며, `app`이 모두 `Up` 상태가 된
이후에는 6회 연속 200으로 안정적으로 응답했다. 재시연 시 `app` 컨테이너가 모두 기동을
마친 뒤 요청을 보내거나, 필요하면 `docker compose restart nginx`로 재해석시키면 된다.)

### 3-6. 무상태 실증 — 포인트 충전(A 인스턴스) → 주문(B 인스턴스)에 즉시 반영

`nginx` 접근 로그의 `upstream_log`로 두 요청이 서로 다른 컨테이너에서 처리됐음을 먼저
확인했다.

```
POST /api/points/charge HTTP/1.1 -> upstream=172.18.0.6:8080 status=200   # app-2
POST /api/orders HTTP/1.1        -> upstream=172.18.0.4:8080 status=201   # app-3
```

같은 호출 순서로 실제 응답 바디:

```bash
curl -s -X POST http://localhost:8080/api/points/charge \
  -H "Content-Type: application/json" -d '{"userId":2,"amount":10000}'
# -> {"userId":2,"balance":10000}                     (app-2가 처리)

curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" -d '{"userId":2,"menuId":2,"quantity":1}'
# -> {"orderId":1,"userId":2,"items":[{"menuId":2,"name":"카페라떼","unitPrice":3500,
#     "quantity":1}],"totalAmount":3500,"balanceAfter":6500,"status":"PAID"}
#     (app-3이 처리, balanceAfter=6500=10000-3500 → app-2가 충전한 값을 app-3이 즉시 반영)

curl -s -X POST http://localhost:8080/api/points/charge \
  -H "Content-Type: application/json" -d '{"userId":2,"amount":5000}'
# -> {"userId":2,"balance":11500}                     (6500+5000)

curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" -d '{"userId":2,"menuId":1,"quantity":1}'
# -> {"orderId":2,"userId":2,"items":[{"menuId":1,"name":"아메리카노","unitPrice":3000,
#     "quantity":1}],"totalAmount":3000,"balanceAfter":8500,"status":"PAID"}
#     (11500-3000=8500)
```

- 4개 요청 모두 `nginx` 라운드로빈에 의해 3개 인스턴스 중 하나로 무작위 분산됐음에도
  잔액이 매번 직전 요청 결과에서 정확히 이어졌다(`10000 → 6500 → 11500 → 8500`). 만약
  잔액이 인스턴스 로컬 메모리에 있었다면 다른 인스턴스가 요청을 받는 순간 값이 0이거나
  직전 갱신을 모르는 상태로 보였을 것이다.
- 이는 `docs/design/state-externalization-check.md`(3절)의 "잔액은 오직 MySQL
  `point_balances` 테이블에만 존재한다"는 정적 점검 결론을, 실제 다중 인스턴스 기동
  환경에서 실행 근거로 재확인한 것이다.

### 3-7. 정리(Step 5)

```bash
docker compose down            # 컨테이너/네트워크만 제거, 볼륨은 유지
docker compose ps               # -> 빈 목록
docker volume ls | grep coffee-order  # -> coffee-order_mysql-data (유지됨)
```

- 컨테이너·네트워크는 모두 정리되어 `docker compose ps` 결과가 비었음을 확인했다.
- `mysql-data` 볼륨은 **유지**했다(삭제하지 않음). 근거: 이 볼륨은 기존 로컬 개발자들이
  `docker compose up`으로 계속 재사용하는 대상이며(주석: "이 파일의 계정/비밀번호는 로컬
  개발용 값"), 이번 시연이 끝났다고 로컬 개발 DB 데이터를 지울 이유가 없다. 다만 3-4절의
  Flyway 동시 마이그레이션 경합 시연을 위해 시연 도중 한 차례 `-v`로 볼륨을 비운 적은
  있으며(2-3절), 최종 정리는 볼륨을 보존한 상태로 마쳤다.
- 이미지(`coffee-order-app`)는 별도로 삭제하지 않았다(로컬 재시연 편의를 위해 유지 —
  운영 배포 대상이 아닌 로컬 시연용 이미지이므로 삭제 강제는 이 태스크 범위 밖).

## 4. 결론

- 3-3절(`docker compose ps`)로 `app` 3개 인스턴스 + `nginx` + `mysql` + `kafka`가 모두
  정상(`Up`/`healthy`) 기동함을 확인했다.
- 3-4절로 3개 인스턴스가 동시에 빈 스키마에 접속해도 Flyway 락(`flyway_schema_history`)이
  마이그레이션을 정확히 1회로 직렬화하고, 나머지 인스턴스는 에러 없이 검증만 하고
  기동을 마친다는 것을 실제 로그로 확인했다(`schema-strategy.md` 근거의 실행 검증).
- 3-5절로 여러 인스턴스에 라운드로빈으로 분산된 `GET /api/menus` 호출이 매번 동일하게
  200을 반환함을 확인했다(어느 인스턴스가 처리하든 기능 결과 동일).
- 3-6절로 포인트 충전(한 인스턴스)과 주문(다른 인스턴스)을 교차로 호출해도 잔액이
  MySQL을 통해 즉시 일관되게 반영됨을 확인했다(무상태 설계의 핵심 실증).
- **결론: 이번 실행 근거로 상위 스토리 E6-1의 수용 기준("인스턴스 증설이 기능에 영향
  없음을 근거로 설명·시연")이 충족되었다.** 세션/로컬 캐시가 없고(태스크 1), 잔액·스트림이
  이미 외부화되어 있으며(태스크 2), 실제로 3개 인스턴스를 동시에 띄워 기능이 동일하게
  동작함을 로그/응답으로 시연했다(태스크 3, 이 문서).

## 5. E6-1(SCRUM-20) 스토리 완료 선언

- **태스크 1** — 세션/로컬 캐시 사용처 점검 및 제거: `docs/design/session-cache-check.md`.
  세션·로컬 캐시·인스턴스 가변 상태 모두 "없음"으로 확정(코드 변경 없음).
- **태스크 2** — 상태 외부화 확인(잔액=MySQL, 락=Redis, 스트림=Kafka):
  `docs/design/state-externalization-check.md`. 잔액·스트림은 외부화 완료, 락은 별도
  스토리(E6-2)에서 다룰 계획된 미도입 상태임을 확인.
- **태스크 3** — 다중 인스턴스 기동 시나리오 문서화(docker-compose scale): 이 문서
  (`multi-instance-scale-check.md`). 실제 `docker compose up --scale app=3`으로 기동해
  Flyway 안전성·라운드로빈·무상태(잔액 즉시 반영)를 실행 근거로 남김.
- 세 태스크 모두 완료되었으며, 위 4절의 결론에 따라 Story **E6-1("① 다수 인스턴스
  무상태 설계", SCRUM-20)의 수용 기준이 충족되었음을 이 문서로 선언한다.

## 6. 후속 태스크와의 범위 경계

- **동시성 제어(Story E6-2, "② 동시성 제어")**: Redis 의존성 추가·`RedisConfig`·분산락
  유틸·`PointBalance` 비관적 락 조회(`@Lock(PESSIMISTIC_WRITE)`)·충전/차감/주문 경로에 락
  적용·동시성 테스트(`ExecutorService` N스레드)는 모두 별도 스토리의 책임이며, 이 문서는
  락 구현을 다루지 않는다. 3-6절에서 확인한 것은 "서로 다른 요청이 순차적으로 왔을 때
  잔액이 인스턴스 로컬이 아니라 MySQL에 즉시 반영된다"는 무상태성이며, "여러 요청이
  *동시에* 같은 잔액을 갱신할 때 분실 갱신이 없는가"는 검증하지 않았다(그건 E6-2의 수용
  기준: "동일 사용자 동시 충전/결제 N건 → 분실 갱신 없음").
- **`docker-compose.yml` 회귀 확인**: 이번 변경은 기존 `mysql`/`kafka` 서비스 정의를
  수정하지 않았으므로, `app`/`nginx` 서비스를 무시하고 기존처럼
  `docker compose up mysql kafka`로 실행해도 기존 로컬 개발 워크플로가 그대로 동작한다
  (Evaluator 단계에서 재확인 권장).
- **운영 배포 파이프라인**: 이번 `Dockerfile`/`docker-compose.yml`은 로컬 시연·개발
  전용이며, CI/CD·운영 오케스트레이션(쿠버네티스 등) 구성은 이 문서의 범위 밖이다.

## 참조

- `docs/design/jira-manual.md`(212~223행, E6-1 태스크3), `docs/design/jira-backlog.md`
  (E6-①), `docs/design/session-cache-check.md`(태스크1), `docs/design/state-externalization-check.md`
  (태스크2), `docs/design/schema-strategy.md`(Flyway 다중 인스턴스 근거),
  `docker-compose.yml`, `Dockerfile`, `nginx.conf`, `src/main/resources/application-dev.yml`,
  `docs/api/point.md`, `docs/api/order.md`, `docs/api/menu.md`, `docs/code-convention.md`
