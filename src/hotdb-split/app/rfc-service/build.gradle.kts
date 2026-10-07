plugins { id("org.springframework.boot") }

dependencies {
    implementation(project(":cdc-debezium"))
    implementation(project(":poll-core"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    runtimeOnly("org.postgresql:postgresql")
    // SAP HANA JDBC — SAP 폴링 · Z 테이블 쓰기 (JCo 가 생기면 RfcSender 만 갈아 끼운다)
    runtimeOnly("com.sap.cloud.db.jdbc:ngdbc:2.25.9")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.awaitility:awaitility")
    testImplementation("org.postgresql:postgresql")
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") { archiveFileName = "app.jar" }
tasks.named<Jar>("jar") { enabled = false }
