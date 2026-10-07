pluginManagement {
    // plugins.gradle.org 가 막힌 망이 있다. Spring Boot · dependency-management 플러그인은 Maven Central 에도 있다
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

rootProject.name = "hotdb"

// cdc-core      : Debezium Embedded + TimescaleDB 청크 역매핑 + 설정 기반 라우팅 (라이브러리)
// zone-service  : 권역별 실적 판별 서비스 — tsdb CDC → svc_* (cdc-core 사용)
// rfc-service  : RFC Service — 판별 모듈 실적 CDC → SAP (2단 CDC) + SAP 폴링 → erp (poll-core)
// db-agent      : DB Agent — Oracle 폴링 → mes · lgs · geo (poll-core)
// poll-core     : 레거시 폴링 라이브러리 (원천 표 → HotDB 레거시 표, 워터마크 증분)
// field-simulator : 필드 데이터 발행기 — HotDB Provider 대신 tsdb 에 직접 적재 (부하 · 테스트용)
// legacy-simulator : 레거시 발행기 — SAP · Oracle 대역의 원천 표를 주기마다 바꿔 폴링이 가져갈 변경분을 만든다 (테스트용)
// hotdb-migrate : Flyway 마이그레이션 실행기 (db/migration · db/legacy)
include("cdc-core", "poll-core", "zone-service", "rfc-service", "db-agent", "field-simulator", "legacy-simulator", "hotdb-migrate")
