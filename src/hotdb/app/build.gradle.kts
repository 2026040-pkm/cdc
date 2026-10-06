plugins {
    java
    id("org.springframework.boot") version "3.5.4" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

val debeziumVersion by extra("3.6.1.Final")

subprojects {
    apply(plugin = "java")
    apply(plugin = "io.spring.dependency-management")

    group = "dev.hotdb"
    version = "0.1.0"

    java {
        toolchain { languageVersion = JavaLanguageVersion.of(21) }
    }

    repositories { mavenCentral() }

    the<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension>().apply {
        imports { mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES) }
    }

    dependencies {
        // Gradle 8.14 + JUnit 5.12 는 런처를 테스트 런타임에 직접 넣어야 테스트를 찾는다
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        // 설정 바인딩이 생성자 파라미터 이름을 쓴다
        options.compilerArgs.add("-parameters")
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        // 통합 테스트(*IT)는 기동 중인 HotDB 스택에 붙는다. -Dhotdb.it=true 일 때만 돈다.
        listOf("hotdb.it", "hotdb.jdbc.url", "hotdb.sim.url").forEach { k ->
            System.getProperty(k)?.let { systemProperty(k, it) }
        }
        testLogging {
            events("passed", "failed", "skipped")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }
}
