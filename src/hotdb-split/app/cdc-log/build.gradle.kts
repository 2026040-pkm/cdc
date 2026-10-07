plugins { `java-library` }

// 캡처부 → 전달부 사이의 계약. 전달부(apply-service)는 이 모듈과 cdc-core 만 안다 — Debezium 이 클래스패스에 없다
dependencies {
    api(project(":cdc-core"))

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
