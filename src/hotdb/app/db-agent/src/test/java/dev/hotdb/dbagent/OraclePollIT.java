package dev.hotdb.dbagent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * DB Agent: Oracle(대역) 의 행이 레거시 DB 표로 옮겨지는지. 기동 중인 db-agent · oracle-sim 이 필요하다.
 * lgs.tracking(위치 추적)은 주기 10초 작업이다. 원천은 Oracle 사용자 LGS 소유 표 (DB Agent 는 LEGACY_READER 로 읽는다).
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class OraclePollIT {

    static final String LEGACY = System.getProperty("hotdb.legacy.url", "jdbc:postgresql://localhost:59435/legacy");
    static final String ORACLE = System.getProperty("hotdb.oracle.url", "jdbc:oracle:thin:@//localhost:59521/FREEPDB1");
    static final Duration WAIT = Duration.ofSeconds(60);
    static final DateTimeFormatter D = DateTimeFormatter.ofPattern("yyyyMMdd");
    static final DateTimeFormatter T = DateTimeFormatter.ofPattern("HHmmss");

    @Test
    void 새_행과_바뀐_행이_워터마크로_옮겨진다() throws SQLException {
        String id = "IT" + UUID.randomUUID().toString().substring(0, 8);
        LocalDateTime now = LocalDateTime.now();
        try (Connection ora = DriverManager.getConnection(ORACLE, "lgs", "lgs")) {
            exec(ora, "INSERT INTO tracking (\"OBJECT_ID\", \"UPD_DATE\", \"UPD_TIME\") VALUES (?, ?, ?)",
                    id, now.format(D), now.format(T));
        }
        try (Connection hot = DriverManager.getConnection(LEGACY, "postgres", "postgres")) {
            await().atMost(WAIT).until(() -> count(hot, "SELECT count(*) FROM lgs.tracking WHERE object_id = ?", id) == 1);

            // 같은 행을 원천에서 고치면 다음 주기에 덮인다 (워터마크가 더 커짐)
            LocalDateTime later = now.plusSeconds(1);
            try (Connection ora = DriverManager.getConnection(ORACLE, "lgs", "lgs")) {
                exec(ora, "UPDATE tracking SET \"UPD_DATE\" = ?, \"UPD_TIME\" = ? WHERE \"OBJECT_ID\" = ?",
                        later.format(D), later.format(T), id);
            }
            await().atMost(WAIT).until(() -> later.format(T).equals(
                    one(hot, "SELECT upd_time FROM lgs.tracking WHERE object_id = ?", id)));

            assertThat(one(hot, "SELECT last_error FROM ops.poll_state WHERE agent = 'db-agent' AND job = 'lgs.tracking'"))
                    .isNull();
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

    static Object one(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getObject(1) : null;
            }
        }
    }

    static long count(Connection c, String sql, Object... args) throws SQLException {
        return ((Number) one(c, sql, args)).longValue();
    }
}
