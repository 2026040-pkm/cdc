# 모든 모듈이 같은 이미지 모양을 쓴다. jar 는 밖에서 빌드해 넣는다 (scripts/up.ps1 → gradlew bootJar).
#   podman build --build-arg MODULE=zone-service -t hotdb/zone-service:local .
FROM docker.io/library/eclipse-temurin:21-jre
ARG MODULE
WORKDIR /app
COPY ${MODULE}/build/libs/app.jar app.jar
RUN mkdir -p /app/data
# 현장 시간대 — JDBC 세션 시간대가 이것을 따른다 (UTC 면 판별 기준일 · 레거시 시각이 9시간 밀린다). 앱도 HOTDB_SITE_TZ 로 다시 맞춘다
ENV TZ=Asia/Seoul
# 메모리는 compose 의 JAVA_TOOL_OPTIONS 로 준다 (podman VM 이 작다)
ENTRYPOINT ["java", "-jar", "app.jar"]
