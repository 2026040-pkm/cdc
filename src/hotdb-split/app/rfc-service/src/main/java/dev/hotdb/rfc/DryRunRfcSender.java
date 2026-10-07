package dev.hotdb.rfc;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SAP 없이 돌 때. 로그만 남긴다 — 보낸 기록(ops.rfc_sent)과 지표는 실제와 같이 쌓인다. */
public class DryRunRfcSender implements RfcSender {

    private static final Logger log = LoggerFactory.getLogger(DryRunRfcSender.class);

    @Override
    public void send(String zone, Map<String, Object> row) {
        log.debug("[dry-run] SAP ← {} {}/{} scan={} {}", zone, row.get("hull_no"), row.get("block_id"),
                row.get("scan_id"), row.get("judged_status"));
    }
}
