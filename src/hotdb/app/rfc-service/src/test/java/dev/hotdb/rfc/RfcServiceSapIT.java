package dev.hotdb.rfc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * RFC Service 의 SAP 쪽 두 방향 — 로컬 SAP 대역(sap-sim)으로 확인한다.
 * <ul>
 *   <li>① SAP 폴링: erpsrc.order_line 에 넣은 행이 레거시 DB erp.order_line 으로 (주기 10초 작업)
 *   <li>② 실적 송신: tsdb COMPLETE → 판별 모듈 → actual_result → (CDC) → erpsrc.zhotdb_actual_result (RFC_SENDER=jdbc)
 * </ul>
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class RfcServiceSapIT {

    static final String SAP = System.getProperty("hotdb.sap.url", "jdbc:postgresql://localhost:59434/sapsim");
    static final String LEGACY = System.getProperty("hotdb.legacy.url", "jdbc:postgresql://localhost:59435/legacy");
    static final Duration WAIT = Duration.ofSeconds(60);

    @Test
    void SAP_원천의_새_행이_erp_로_옮겨진다() throws SQLException {
        String req = "IT" + UUID.randomUUID().toString().substring(0, 8);
        LocalDateTime now = LocalDateTime.now();
        try (Connection sap = DriverManager.getConnection(SAP, "sapsim", "sapsim")) {
            exec(sap, "INSERT INTO erpsrc.order_line (order_no, line_no, upd_date, upd_time) VALUES (?, '0001', ?, ?)",
                    req, now.format(DateTimeFormatter.ofPattern("yyyyMMdd")), now.format(DateTimeFormatter.ofPattern("HHmmss")));
        }
        try (Db db = Db.at(LEGACY, "postgres")) {
            await().atMost(WAIT).until(() -> db.count("SELECT count(*) FROM erp.order_line WHERE order_no = ?", req) == 1);
            assertThat(db.one("SELECT last_error FROM ops.poll_state WHERE agent = 'rfc-service' AND job = 'erp.order_line'")).isNull();
        }
    }

    @Test
    void 판별_모듈의_실적이_SAP_Z_표에_한_번_쓰인다() throws SQLException {
        String run = UUID.randomUUID().toString().substring(0, 8);
        String device = "IT-SAP-" + run;
        String hull = "S" + run;
        UUID scan = UUID.randomUUID();
        try (Db p = Db.as("hotdb_provider")) {
            p.update("INSERT INTO tsdb.device (site, device_id, device_role, zone) VALUES ('it', ?, 'LIDAR', 'ASSEMBLY')", device);
            p.update("""
                    INSERT INTO tsdb.tag_catalog (edge_group_id, tag_id, mqtt_topic, source_id, site, device_id, channel)
                    VALUES ('it', ?, 'it', ?, 'it', ?, 'actual')""", device + ".actual", device, device);
            long tag = p.count("SELECT tag_key FROM tsdb.tag_catalog WHERE edge_group_id = 'it' AND tag_id = ?", device + ".actual");
            p.update("""
                    INSERT INTO tsdb.actual_history (event_time, tag_key, scan_id, event_type, hull_no, block_id,
                      block_progress_rate, match_confidence, source_time_text, received_at)
                    VALUES (?, ?, ?, 'COMPLETE', ?, 'B1', 100, 0.93, 'it', clock_timestamp())""",
                    OffsetDateTime.now(), tag, scan, hull);
        }
        try (Connection sap = DriverManager.getConnection(SAP, "sapsim", "sapsim")) {
            await().atMost(WAIT).until(() -> count(sap, "SELECT count(*) FROM erpsrc.zhotdb_actual_result WHERE hull_no = ?", hull) == 1);
            try (PreparedStatement ps = sap.prepareStatement(
                    "SELECT zone, judged_status, device_id, block_progress_rate FROM erpsrc.zhotdb_actual_result WHERE hull_no = ?")) {
                ps.setString(1, hull);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertThat(rs.getString("zone")).isEqualTo("asm");
                    assertThat(rs.getString("judged_status")).isEqualTo("PENDING");
                    assertThat(rs.getString("device_id")).isEqualTo(device);
                    assertThat(rs.getDouble("block_progress_rate")).isEqualTo(100.0);
                }
            }
        }
    }

    static void exec(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            ps.executeUpdate();
        }
    }

    static long count(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}
