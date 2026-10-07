package dev.hotdb.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * tsdb COMPLETE → (CDC) → actual_result PENDING → (판별 단계) → CONFIRMED · REJECTED.
 *
 * <p>기동 중인 zone-asm(기본 임계값 규칙: 진척률 100 · 정합도 0.9)이 필요하다.
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class JudgementIT {

    static final Duration WAIT = Duration.ofSeconds(30);
    static final String DEVICE = "IT-JDG-" + UUID.randomUUID().toString().substring(0, 8);
    static long actualTag;

    @BeforeAll
    static void 장비를_등록한다() throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            actualTag = ZoneCdcE2eIT.register(p, DEVICE, "ASSEMBLY", "actual", null);
        }
    }

    @Test
    void 임계를_넘으면_확정되고_판정_시각이_남는다() throws SQLException {
        UUID scan = UUID.randomUUID();
        complete(scan, 100, 0.95);

        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> "CONFIRMED".equals(status(db, scan)));
            assertThat(db.one("SELECT judged_at FROM svc.actual_result WHERE module = 'asm' AND scan_id = ?", scan))
                    .isNotNull();
        }
    }

    @Test
    void 정합도가_모자라면_반려() throws SQLException {
        UUID scan = UUID.randomUUID();
        complete(scan, 100, 0.5);

        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> "REJECTED".equals(status(db, scan)));
        }
    }

    @Test
    void 같은_실적이_다시_와도_판정은_되돌아가지_않는다() throws SQLException {
        UUID scan = UUID.randomUUID();
        complete(scan, 100, 0.95);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> "CONFIRMED".equals(status(db, scan)));
            Object judgedAt = db.one("SELECT judged_at FROM svc.actual_result WHERE module = 'asm' AND scan_id = ?", scan);

            complete(scan, 100, 0.95);   // 재전송 — 라우트가 judged_status 를 keep
            await().atMost(WAIT).until(() -> db.count("""
                    SELECT count(*) FROM svc.actual_result
                     WHERE module = 'asm' AND scan_id = ? AND applied_at > judged_at""", scan) == 1);
            assertThat(status(db, scan)).isEqualTo("CONFIRMED");
            assertThat(db.one("SELECT judged_at FROM svc.actual_result WHERE module = 'asm' AND scan_id = ?", scan))
                    .isEqualTo(judgedAt);
        }
    }

    static String status(Db db, UUID scan) throws SQLException {
        return (String) db.one("SELECT judged_status FROM svc.actual_result WHERE module = 'asm' AND scan_id = ?", scan);
    }

    static void complete(UUID scan, double progress, double confidence) throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            p.update("""
                    INSERT INTO tsdb.actual_history (event_time, tag_key, scan_id, event_type, hull_no, block_id,
                      block_progress_rate, match_confidence, source_time_text, received_at)
                    VALUES (?, ?, ?, 'COMPLETE', 'H1', 'B1', ?, ?, 'it', clock_timestamp())
                    """, OffsetDateTime.now(), actualTag, scan, (float) progress, (float) confidence);
        }
    }
}
