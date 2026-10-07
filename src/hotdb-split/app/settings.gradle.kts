pluginManagement {
    // plugins.gradle.org 가 막힌 망이 있다. Spring Boot · dependency-management 플러그인은 Maven Central 에도 있다
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

rootProject.name = "hotdb-split"

// ── 분리판 (src/hotdb 의 복사본에서 캡처부 · 전달부를 프로세스로 나눈 비교용) ──────────────
// cdc-core      : 엔진에 매이지 않는 몫 — 포트(CdcSource · CdcSink · SourceCapabilities) · 라우팅 · 반영 · 슬롯 연속성
// cdc-debezium  : CdcSource 의 Debezium Embedded 어댑터 (엔진을 바꾸면 이 모듈만 바꾼다)
// cdc-log       : 캡처 → 전달 사이 변경 로그 계약(ChangeRecord · 선 형식 · /log HTTP) + 전달 쪽 어댑터(log 당겨오기)
// capture-service : 캡처부 — 슬롯 → 변경 로그(로컬 파일) · /log 로 내준다. 라우트 · 판별 없음
// apply-service : 전달부 — zone-service 와 같은 소스 · 설정, 원천만 cdc-log (엔진 없음)
// zone-service  : 권역별 실적 판별 서비스 — tsdb CDC → svc_* (cdc-core 사용)
// rfc-service  : RFC Service — 판별 모듈 실적 CDC → SAP (2단 CDC) + SAP 폴링 → erp (poll-core)
// db-agent      : DB Agent — Oracle 폴링 → mes · lgs · geo (poll-core)
// poll-core     : 레거시 폴링 라이브러리 (원천 표 → HotDB 레거시 표, 워터마크 증분)
// field-simulator : 필드 데이터 발행기 — HotDB Provider 대신 tsdb 에 직접 적재 (부하 · 테스트용)
// legacy-simulator : 레거시 발행기 — SAP · Oracle 대역의 원천 표를 주기마다 바꿔 폴링이 가져갈 변경분을 만든다 (테스트용)
// hotdb-migrate : Flyway 마이그레이션 실행기 (db/migration · db/legacy)
include("cdc-core", "cdc-debezium", "cdc-log", "capture-service", "apply-service", "poll-core", "zone-service", "rfc-service", "db-agent", "field-simulator", "legacy-simulator", "hotdb-migrate")
