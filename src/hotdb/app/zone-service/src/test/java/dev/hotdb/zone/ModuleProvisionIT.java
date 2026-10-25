package dev.hotdb.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 판별 모듈은 늘 수 있다 — ops.provision_module 한 줄로 계정 · 등록부가 생기고, 공종(trade)을 달면 판별이 그 앵커를 찾는지.
 * V17 부터 모듈 RDB 는 hot 스키마 하나이고 표는 모듈과 상관없다. 실행마다 이름이 다른 모듈을 만들고 끝에 걷어낸다.
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class ModuleProvisionIT {

    static final String MODULE = "it" + UUID.randomUUID().toString().substring(0, 6).replaceAll("[^a-z0-9]", "x");
    static final String ROLE = "svc_" + MODULE;
    static final String DEVICE = "IT-MOD-" + MODULE;

    @AfterAll
    static void 걷어낸다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            db.update("DELETE FROM hot.m_device_state WHERE device_id = ?", DEVICE);
            db.update("DELETE FROM hot.m_device WHERE device_id = ?", DEVICE);
            db.one("SELECT ops.drop_module(?)::text", MODULE);
            assertThat(db.count("SELECT count(*) FROM pg_roles WHERE rolname = ?", ROLE)).isZero();
        }
    }

    @Test
    void 모듈_하나를_한_줄로_만들고_공종을_달면_판별_사전이_그_모듈을_안다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            db.one("SELECT ops.provision_module(?, ?, '시험 판별')::text", MODULE, "IT_" + MODULE.toUpperCase());
            db.one("SELECT ops.provision_module(?, ?, '시험 판별')::text", MODULE, "IT_" + MODULE.toUpperCase());  // 두 번 불러도 같다
            db.update("UPDATE ops.module SET trade = 'S' WHERE module = ?", MODULE);

            assertThat(db.one("SELECT zone_code FROM ops.module WHERE module = ?", MODULE)).isEqualTo("IT_" + MODULE.toUpperCase());
            assertThat(db.count("SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member "
                    + "JOIN pg_roles g ON g.oid = m.roleid WHERE r.rolname = ? AND g.rolname IN ('svc_reader', 'svc_writer')", ROLE))
                    .as("공통 읽기 · 모듈 쓰기 역할").isEqualTo(2);
            assertThat(db.count("SELECT count(*) FROM ops.module_apply_lag() WHERE zone = ?", MODULE))
                    .as("모니터링 함수가 새 모듈을 알아서 센다").isEqualTo(1);
        }
        try (Db m = Db.as(ROLE)) {
            // 새 모듈 계정은 hot 의 장비 · 상태 · 스캔 · 실적을 쓸 수 있고, 레거시 참조 표는 읽기만
            m.update("""
                    INSERT INTO hot.m_device (device_id, class_code, category, zone)
                    VALUES (?, 'LIDAR', 'SENSOR', ?)""", DEVICE, "IT_" + MODULE.toUpperCase());
            m.update("INSERT INTO hot.m_device_state (device_id, online, updated_at) VALUES (?, true, now())", DEVICE);
            assertThat(m.count("SELECT count(*) FROM hot.m_device_state WHERE device_id = ?", DEVICE)).isEqualTo(1);
            assertThatThrownBy(() -> m.update("INSERT INTO hot.l_event (unit_id, event_group, event_code, source, event_ts) "
                    + "VALUES ('x', 'EVENT', 'CARY_DUE', 'it', now())")).hasMessageContaining("permission denied");
        }
    }
}
