plugins { id("org.springframework.boot") }

dependencies {
    implementation(project(":poll-core"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("com.oracle.database.jdbc:ojdbc11")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.awaitility:awaitility")
    testImplementation("org.postgresql:postgresql")
    testImplementation("com.oracle.database.jdbc:ojdbc11")
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") { archiveFileName = "app.jar" }
tasks.named<Jar>("jar") { enabled = false }
