plugins { `java-library` }

dependencies {
    // SqlBuilder · SqlValues · Catalog 를 같이 쓴다 (CDC 엔진은 hotdb.cdc.pipeline 이 없으면 켜지지 않는다)
    api(project(":cdc-core"))
    api("org.springframework.boot:spring-boot-starter-jdbc")
    api("org.springframework.boot:spring-boot-starter-actuator")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
