package dev.hotdb.cdc.continuity;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 캡처 연결고리가 끊겼는지 기동 때 판정한다 (이전 Java 판 embedded-cdc 의 V3 검증 그대로).
 *
 * <p>Debezium 은 "오프셋 파일은 있는데 슬롯이 없다"는 스스로 잡는다. 하지만 근거가 오프셋 파일이라
 * <b>오프셋 볼륨이 함께 사라지면 알아채지 못하고</b> 처음 기동으로 보고 새 슬롯을 만든다 — 그 사이 변경은 조용히 빠진다.
 * 그래서 DB 에 남긴 처리 위치({@link CdcCheckpoints})를 슬롯과 대조한다. 셋 중 하나면 되받을 수 없는 구간이 있다:
 * <ol>
 *   <li>처리 이력은 있는데 슬롯이 없다 — 지워졌거나 DB 가 바뀌었다
 *   <li>슬롯이 무효화됐다 (wal_status = lost) — max_slot_wal_keep_size 를 넘었다
 *   <li>슬롯의 restart_lsn 이 처리한 위치보다 앞에 있다 — 그 사이 WAL 은 이미 버렸다
 * </ol>
 */
public class SlotContinuityGuard {

    private static final Logger log = LoggerFactory.getLogger(SlotContinuityGuard.class);

    private final String pipeline;
    private final String slot;
    private final JdbcTemplate jdbc;

    public SlotContinuityGuard(String pipeline, String slot, JdbcTemplate jdbc) {
        this.pipeline = pipeline;
        this.slot = slot;
        this.jdbc = jdbc;
    }

    /** @return 되받을 수 없는 구간이 있으면 그 설명 */
    public Optional<String> detectGap() {
        record Row(String checkpoint, boolean slotExists, String restart, String walStatus, boolean behind) {}
        Optional<Row> row = jdbc.query("""
                SELECT c.last_applied_lsn::text, s.slot_name IS NOT NULL, s.restart_lsn::text, s.wal_status,
                       coalesce(s.restart_lsn > c.last_applied_lsn, false)
                  FROM ops.cdc_checkpoint c
                  LEFT JOIN pg_replication_slots s ON s.slot_name = ?
                 WHERE c.pipeline = ?
                """, rs -> rs.next()
                ? Optional.of(new Row(rs.getString(1), rs.getBoolean(2), rs.getString(3), rs.getString(4), rs.getBoolean(5)))
                : Optional.<Row>empty(), slot, pipeline);

        if (row.isEmpty()) {
            log.info("처리 이력이 없다 — 처음 기동으로 본다 (pipeline={})", pipeline);
            return Optional.empty();
        }
        Row r = row.get();
        if (!r.slotExists()) {
            return gap("처리 이력(LSN " + r.checkpoint() + ")은 있는데 복제 슬롯 '" + slot + "' 이 없다. 슬롯이 지워졌거나 DB 가 바뀌었다");
        }
        if ("lost".equals(r.walStatus())) {
            return gap("복제 슬롯 '" + slot + "' 이 무효화됐다(wal_status=lost) — max_slot_wal_keep_size 를 넘겨 WAL 을 버렸다");
        }
        if (r.behind()) {
            return gap("슬롯이 이미 " + r.restart() + " 까지 버렸는데 처리한 위치는 " + r.checkpoint() + " 이다. 그 사이는 되받을 수 없다");
        }
        log.info("캡처 연결고리 정상 — 처리 위치 {} / 슬롯 restart_lsn {}", r.checkpoint(), r.restart());
        return Optional.empty();
    }

    private Optional<String> gap(String reason) {
        return Optional.of(reason + " — 이 구간은 WAL 로 복구할 수 없다. 대상 표를 다시 맞춘(재동기화) 뒤 "
                + "DELETE FROM ops.cdc_checkpoint WHERE pipeline = '" + pipeline + "' 하고 재기동한다");
    }
}
