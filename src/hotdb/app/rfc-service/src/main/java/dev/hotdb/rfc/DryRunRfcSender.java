package dev.hotdb.rfc;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SAP 없이 돌 때. 로그만 남긴다 — 보낸 기록(ops.rfc_progress_sent)과 지표는 실제와 같이 쌓인다. */
public class DryRunRfcSender implements RfcSender {

    private static final Logger log = LoggerFactory.getLogger(DryRunRfcSender.class);

    @Override
    public void send(Map<String, Object> row) {
        log.debug("[dry-run] SAP ← {} {}/{} as_of={} {} auto={} legacy={} cp={}", row.get("anchor_type"),
                row.get("hull_no"), row.get("block_no"), row.get("as_of"), row.get("state"), row.get("auto_pct"),
                row.get("legacy_pct"), row.get("last_cp"));
    }
}
