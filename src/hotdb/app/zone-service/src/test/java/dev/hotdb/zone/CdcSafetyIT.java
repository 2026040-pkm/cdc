package dev.hotdb.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.hotdb.cdc.continuity.SlotContinuityGuard;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 이전 Java 판(embedded-cdc)에서 옮긴 안전장치가 실제 스택에서 도는지 — 처리 위치 기록 · 캡처 갭 · dead letter 재처리.
 * 기동 중인 스택(HotDB + zone-asm)이 필요하다.
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class CdcSafetyIT {

    static final String RUN = UUID.randomUUID().toString().substring(0, 8);

    @Test
    void 처리_위치가_슬롯을_따라_전진한다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            await().atMost(Duration.ofSeconds(30)).until(() -> db.count("""
                    SELECT count(*) FROM ops.cdc_checkpoint
                     WHERE pipeline = 'zone-asm' AND slot_name = 'zone_asm' AND updated_at > now() - interval '15 seconds'
                    """) == 1);
            // 체크포인트가 슬롯이 버린 위치보다 뒤에 있으면 다음 기동에서 캡처 갭으로 오인한다
            assertThat(db.one("""
                    SELECT c.last_applied_lsn >= s.restart_lsn
                      FROM ops.cdc_checkpoint c JOIN pg_replication_slots s ON s.slot_name = c.slot_name
                     WHERE c.pipeline = 'zone-asm'
                    """)).isEqualTo(true);
        }
    }

    @Test
    void 슬롯이_없거나_처리_위치보다_앞서면_캡처_갭이다() throws SQLException {
        String pipeline = "it-gap-" + RUN;
        String slot = "it_gap_" + RUN.replace('-', '_');
        DriverManagerDataSource ds = new DriverManagerDataSource(Db.URL, "postgres", "postgres");
        SlotContinuityGuard guard = new SlotContinuityGuard(pipeline, slot, new JdbcTemplate(ds));
        try (Db db = Db.as("postgres")) {
            assertThat(guard.detectGap()).as("처리 이력이 없으면 처음 기동").isEmpty();

            db.update("INSERT INTO ops.cdc_checkpoint VALUES (?, ?, '0/1', now())", pipeline, slot);
            assertThat(guard.detectGap()).as("이력은 있는데 슬롯이 없다").hasValueSatisfying(
                    g -> assertThat(g).contains("슬롯 '" + slot + "' 이 없다"));

            db.rows("SELECT pg_create_logical_replication_slot(?, 'pgoutput')", slot);
            try {
                assertThat(guard.detectGap()).as("슬롯이 처리 위치(0/1)보다 앞을 이미 버렸다").hasValueSatisfying(
                        g -> assertThat(g).contains("까지 버렸는데"));

                db.update("""
                        UPDATE ops.cdc_checkpoint
                           SET last_applied_lsn = (SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = ?)
                         WHERE pipeline = ?""", slot, pipeline);
                assertThat(guard.detectGap()).as("슬롯이 처리 위치부터 갖고 있으면 정상").isEmpty();
            } finally {
                db.rows("SELECT pg_drop_replication_slot(?)", slot);
                db.update("DELETE FROM ops.cdc_checkpoint WHERE pipeline = ?", pipeline);
            }
        }
    }

    @Test
    void 재처리_신청한_dead_letter_만_다시_반영하고_더_새_값은_덮지_않는다() throws SQLException {
        String device = "IT-ASM-DLQ-" + RUN;
        long tag;
        try (Db p = Db.as("hotdb_provider")) {
            tag = ZoneCdcE2eIT.register(p, device, "ASSEMBLY", "status", null);
        }
        OffsetDateTime t0 = OffsetDateTime.now(ZoneOffset.UTC).withNano(0);
        ZoneCdcE2eIT.status(tag, t0, "ONLINE", null);   // 정상 경로로 한 번 — 모듈의 태그 캐시에 이 장비가 올라온다

        try (Db db = Db.as("postgres")) {
            await().atMost(Duration.ofSeconds(30)).until(() -> "ONLINE".equals(
                    db.one("SELECT status FROM svc.device_status_current WHERE module = 'asm' AND device_id = ?", device)));

            long newer = deadLetter(db, tag, t0.plusSeconds(10), "ERROR", "PENDING");
            long older = deadLetter(db, tag, t0.plusSeconds(5), "ONLINE", "PENDING");
            Thread.sleep(500);
            assertThat(db.one("SELECT status FROM ops.cdc_dead_letter WHERE id = ?", newer))
                    .as("PENDING 은 자동으로 집지 않는다").isEqualTo("PENDING");

            db.update("UPDATE ops.cdc_dead_letter SET status = 'RETRY_REQUESTED' WHERE id IN (?, ?)", newer, older);
            await().atMost(Duration.ofSeconds(75)).until(() -> db.count(
                    "SELECT count(*) FROM ops.cdc_dead_letter WHERE id IN (?, ?) AND status = 'RESOLVED'", newer, older) == 2);

            assertThat(db.one("SELECT resolution FROM ops.cdc_dead_letter WHERE id = ?", newer)).isEqualTo("APPLIED");
            assertThat(db.one("SELECT resolution FROM ops.cdc_dead_letter WHERE id = ?", older))
                    .as("더 새 값(10초)이 이미 있어 newer-than 에 막힌다").isEqualTo("STALE_SKIPPED");
            assertThat(db.one("SELECT status FROM svc.device_status_current WHERE module = 'asm' AND device_id = ?", device))
                    .isEqualTo("ERROR");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** status-current 라우트에서 격리됐던 것처럼 Debezium 원문을 넣는다. */
    static long deadLetter(Db db, long tag, OffsetDateTime at, String status, String state) throws SQLException {
        String raw = """
                {"before":null,
                 "after":{"event_time":"%s","tag_key":%d,"status":"%s","error_code":null,"received_at":"%s"},
                 "source":{"schema":"tsdb","table":"status_history","lsn":1,"ts_ms":%d},
                 "op":"c","ts_ms":%d}
                """.formatted(at, tag, status, OffsetDateTime.now(ZoneOffset.UTC), at.toInstant().toEpochMilli(),
                at.toInstant().toEpochMilli());
        return ((Number) db.one("""
                INSERT INTO ops.cdc_dead_letter (pipeline, route, source, lsn, payload, error, status)
                VALUES ('zone-asm', 'status-current', 'tsdb.status_history', 1, CAST(? AS jsonb), 'it', ?)
                RETURNING id""", raw, state)).longValue();
    }
}
