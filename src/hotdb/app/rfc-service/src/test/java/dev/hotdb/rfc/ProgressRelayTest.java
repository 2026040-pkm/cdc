package dev.hotdb.rfc;

import static org.assertj.core.api.Assertions.assertThat;

import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ProgressRelayTest {

    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final ProgressRelay relay = new ProgressRelay("rfc-service", new RfcProperties(0, 0, "dry-run", null), null, null,
            new DryRunRfcSender(), meters);

    @Test
    void hot_의_p_progress_만_받는다() {
        assertThat(relay.interestedIn(new TableId("hot", "p_progress"))).isTrue();
        assertThat(relay.interestedIn(new TableId("hot", "p_progress_block"))).as("블록 롤업은 SAP 로 안 간다").isFalse();
        assertThat(relay.interestedIn(new TableId("svc", "actual_result"))).as("V17 전의 PoC 표").isFalse();
        assertThat(relay.interestedIn(new TableId("hot", "s_scan_detection"))).isFalse();
        assertThat(relay.interestedIn(new TableId("ops", "cdc_heartbeat"))).isFalse();
    }

    @Test
    void 내용_해시는_숫자_표기를_맞추고_진척이_바뀌면_달라진다() {
        Map<String, Object> a = new java.util.HashMap<>(Map.of("state", "inProgress", "auto_pct", 40, "legacy_pct", "35.50"));
        Map<String, Object> b = new java.util.HashMap<>(Map.of("state", "inProgress", "auto_pct", "40.0", "legacy_pct", 35.5));
        assertThat(ProgressRelay.payloadHash(a)).as("40 · 40.0 · 35.50 · 35.5 는 같은 값").isEqualTo(ProgressRelay.payloadHash(b));
        b.put("auto_pct", 70);
        assertThat(ProgressRelay.payloadHash(a)).as("같은 상태에서 진척만 바뀜 (V20 F3)").isNotEqualTo(ProgressRelay.payloadHash(b));
        Map<String, Object> c = new java.util.HashMap<>(a);
        c.put("last_cp", "CUT_DONE");
        assertThat(ProgressRelay.payloadHash(a)).isNotEqualTo(ProgressRelay.payloadHash(c));
        assertThat(ProgressRelay.payloadHash(a)).hasSize(16);
    }

    @Test
    void 제외_앵커와_삭제는_보내지_않고_센다() {
        AtomicInteger sent = new AtomicInteger();
        ProgressRelay r = new ProgressRelay("rfc-service", new RfcProperties(0, 0, "dry-run", null), null, null,
                row -> sent.incrementAndGet(), meters);
        Map<String, Object> excluded = Map.of("anchor_id", "5486DS914CNG01", "as_of", "2026-10-23", "state", "notDue",
                "excluded", true, "anchor_type", "DWG");
        r.apply(List.of(event(Op.CREATE, excluded), event(Op.DELETE, excluded)));
        assertThat(sent.get()).isZero();
        assertThat(meters.counter("hotdb.rfc.skipped", "reason", "excluded").count()).isEqualTo(1);
        assertThat(meters.counter("hotdb.rfc.skipped", "reason", "delete").count()).isEqualTo(1);
    }

    @Test
    void 기준일은_epoch_일수로_와도_ISO_날짜로_바꾼다() {
        assertThat(ProgressRelay.asOf(20749)).isEqualTo("2026-10-23");
        assertThat(ProgressRelay.asOf("2026-10-23")).isEqualTo("2026-10-23");
        assertThat(ProgressRelay.asOf(null)).isNull();
    }

    static CdcEvent event(Op op, Map<String, Object> row) {
        TableId t = new TableId("hot", "p_progress");
        return new CdcEvent(t, t, op, op == Op.DELETE ? row : null, op == Op.DELETE ? null : row, 1L, null, "{}");
    }
}
