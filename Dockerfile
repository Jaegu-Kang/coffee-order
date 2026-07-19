# E6-1 태스크3(다중 인스턴스 기동 시나리오 문서화) 시연 전용 이미지.
# docs/design/multi-instance-scale-check.md 참고. 도메인 코드는 변경하지 않고,
# 기존 build.gradle의 bootJar 산출물을 그대로 컨테이너에 담아 실행하기만 한다.
#
# 1단계: Gradle 빌드 스테이지 — 저장소의 gradlew/build.gradle을 그대로 사용해 bootJar 생성.
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace
COPY gradlew ./
COPY gradle ./gradle
COPY build.gradle settings.gradle ./
COPY src ./src
RUN chmod +x gradlew && ./gradlew bootJar --no-daemon -x test

# 2단계: 실행 스테이지 — JRE만 포함한 경량 이미지에 jar만 복사해 실행.
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
