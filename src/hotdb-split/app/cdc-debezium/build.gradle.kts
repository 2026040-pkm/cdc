plugins { `java-library` }

val debeziumVersion: String by rootProject.extra

// CdcSource 의 Debezium Embedded 어댑터. 엔진을 바꾸면 이 모듈 대신 다른 어댑터 모듈을 의존한다
// (pgoutput 직접 수신 · 버전 컬럼 폴링 — 계약은 cdc-core 의 port.CdcSource)
dependencies {
    api(project(":cdc-core"))
    api("io.debezium:debezium-api:$debeziumVersion")
    api("io.debezium:debezium-embedded:$debeziumVersion") {
        // 옛 log4j 계열 바인딩이 Boot 의 logback 과 충돌한다
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "ch.qos.reload4j", module = "reload4j")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
    }
    api("io.debezium:debezium-connector-postgres:$debeziumVersion")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
