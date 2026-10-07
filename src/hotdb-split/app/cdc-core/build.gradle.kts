plugins { `java-library` }

// 엔진에 매이지 않는 몫 — 포트(CdcSource · CdcSink) · 이벤트 · 라우팅 · 반영 · 슬롯 연속성.
// 엔진(Debezium)은 cdc-debezium 에 따로 있다. 여기에 엔진 의존을 더하지 않는다 (PortBoundaryTest)
dependencies {
    api("org.springframework.boot:spring-boot-starter-jdbc")
    api("org.springframework.boot:spring-boot-starter-actuator")
    api("io.micrometer:micrometer-registry-prometheus")
    api("com.fasterxml.jackson.core:jackson-databind")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
