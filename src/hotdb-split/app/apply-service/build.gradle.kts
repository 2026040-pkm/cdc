plugins { id("org.springframework.boot") }

// 전달부 — 판별 모듈(zone-service)과 같은 소스 · 같은 설정을 그대로 빌드하고, 원천만 Debezium 대신 cdc-log 로 바꾼다.
// 판별 모듈 코드는 한 줄도 다르지 않다: 엔진이 바뀌어도(분리 · 교체) 전달부가 그대로라는 것이 이 모듈의 증명이다.
// Debezium 은 클래스패스에 없다 (ApplyClasspathTest).
sourceSets {
    main {
        java.srcDir("../zone-service/src/main/java")
        resources.srcDir("../zone-service/src/main/resources")
    }
}

dependencies {
    implementation(project(":cdc-log"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveFileName = "app.jar"
    mainClass = "dev.hotdb.zone.ZoneServiceApplication"
}
tasks.named<Jar>("jar") { enabled = false }
// 두 자원 디렉터리에 같은 이름(application.yml)이 있으면 apply-service 쪽(config/)이 이긴다 — 아래는 겹치는 파일 처리만
tasks.named<ProcessResources>("processResources") { duplicatesStrategy = DuplicatesStrategy.EXCLUDE }
