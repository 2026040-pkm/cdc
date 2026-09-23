# ── 1단계: 빌드 ──────────────────────────────────────────────────────────────
FROM gradle:8.14-jdk21 AS build
WORKDIR /workspace

COPY build.gradle.kts settings.gradle.kts ./
RUN gradle dependencies --no-daemon > /dev/null 2>&1 || true

COPY src ./src
RUN gradle bootJar --no-daemon

# ── 2단계: 실행 ──────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/build/libs/*.jar app.jar

# 컨테이너 안에서 호스트의 Aspire NATS 에 붙으려면 NATS_URL 의 호스트를 host.containers.internal
# (docker 는 host.docker.internal)로 준다. Aspire 포트는 127.0.0.1 에만 열려 있다는 점에 유의.
EXPOSE 64080
ENTRYPOINT ["java", "-jar", "app.jar"]
