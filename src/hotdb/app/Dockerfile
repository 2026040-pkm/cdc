# 모든 모듈이 같은 이미지 모양을 쓴다. jar 는 밖에서 빌드해 넣는다 (scripts/up.ps1 → gradlew bootJar).
#   podman build --build-arg MODULE=zone-service -t hotdb/zone-service:local .
FROM docker.io/library/eclipse-temurin:21-jre
ARG MODULE
WORKDIR /app
COPY ${MODULE}/build/libs/app.jar app.jar
RUN mkdir -p /app/data
# 메모리는 compose 의 JAVA_TOOL_OPTIONS 로 준다 (podman VM 이 작다)
ENTRYPOINT ["java", "-jar", "app.jar"]
