plugins { `java-library` }

val debeziumVersion: String by rootProject.extra

dependencies {
    api("org.springframework.boot:spring-boot-starter-jdbc")
    api("org.springframework.boot:spring-boot-starter-actuator")
    api("io.micrometer:micrometer-registry-prometheus")
    api("com.fasterxml.jackson.core:jackson-databind")

    api("io.debezium:debezium-api:$debeziumVersion")
    api("io.debezium:debezium-embedded:$debeziumVersion") {
        // 옛 log4j 계열 바인딩이 Boot 의 logback 과 충돌한다
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "ch.qos.reload4j", module = "reload4j")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
    }
    api("io.debezium:debezium-connector-postgres:$debeziumVersion")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
