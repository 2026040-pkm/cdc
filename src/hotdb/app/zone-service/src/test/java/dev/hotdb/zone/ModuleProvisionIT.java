package dev.hotdb.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 판별 모듈은 늘 수 있다 — ops.provision_module 한 줄로 계정 · 등록부가 생기고, 행 보안(RLS)이 그 모듈 행만 쓰게 하는지.
 * 판별 모듈 RDB 는 스키마 svc 하나(V9)라 표는 새로 생기지 않는다. 실행마다 이름이 다른 모듈을 만들고 끝에 걷어낸다.
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class ModuleProvisionIT {

    static final String MODULE = "it" + UUID.randomUUID().toString().substring(0, 6).replaceAll("[^a-z0-9]", "x");
    static final String ROLE = "svc_" + MODULE;
    static final String DEVICE = "IT-MOD-" + MODULE;

    @AfterAll
    static void 걷어낸다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            db.one("SELECT ops.drop_module(?)::text", MODULE);
            assertThat(db.count("SELECT count(*) FROM pg_roles WHERE rolname = ?", ROLE)).isZero();
            assertThat(db.count("SELECT count(*) FROM svc.device_status_current WHERE module = ?", MODULE))
                    .as("그 모듈의 행도 같이 지워진다").isZero();
        }
    }

    @Test
    void 모듈_하나를_한_줄로_만들고_그_모듈_행만_쓴다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            db.one("SELECT ops.provision_module(?, ?, '시험 판별')::text", MODULE, "IT_" + MODULE.toUpperCase());
            db.one("SELECT ops.provision_module(?, ?, '시험 판별')::text", MODULE, "IT_" + MODULE.toUpperCase());  // 두 번 불러도 같다

            assertThat(db.one("SELECT zone_code FROM ops.module WHERE module = ?", MODULE)).isEqualTo("IT_" + MODULE.toUpperCase());
            assertThat(db.count("SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member "
                    + "JOIN pg_roles g ON g.oid = m.roleid WHERE r.rolname = ? AND g.rolname IN ('svc_reader', 'svc_writer')", ROLE))
                    .as("공통 읽기 · 모듈 쓰기 역할").isEqualTo(2);
            assertThat(db.count("SELECT count(*) FROM ops.module_apply_lag() WHERE zone = ?", MODULE))
                    .as("모니터링 함수가 새 모듈을 알아서 센다").isEqualTo(1);
            db.update("INSERT INTO tsdb.device (site, device_id, device_role, zone) VALUES ('it', ?, 'LIDAR', ?)",
                    DEVICE, "IT_" + MODULE.toUpperCase());
        }
        try (Db m = Db.as(ROLE)) {
            m.update("""
                    INSERT INTO svc.device_status_current (module, site, device_id, status, src_event_time, src_received_at, applied_at)
                    VALUES (?, 'it', ?, 'ONLINE', now(), now(), now())""", MODULE, DEVICE);
            assertThat(m.count("SELECT count(*) FROM svc.device_status_current")).as("자기 행만 보인다").isEqualTo(1);
            assertThatThrownBy(() -> m.update("UPDATE svc.device_status_current SET module = 'asm' WHERE device_id = ?", DEVICE))
                    .hasMessageContaining("row-level security");
        }
    }
}
