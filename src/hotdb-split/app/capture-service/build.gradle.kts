plugins { id("org.springframework.boot") }

// 캡처부 — 엔진(cdc-debezium) + 변경 로그 계약(cdc-log). 라우트 · 판별 코드는 없다
dependencies {
    implementation(project(":cdc-debezium"))
    implementation(project(":cdc-log"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") { archiveFileName = "app.jar" }
tasks.named<Jar>("jar") { enabled = false }
