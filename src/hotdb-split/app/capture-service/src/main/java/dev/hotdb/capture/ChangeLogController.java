package dev.hotdb.capture;

import dev.hotdb.cdc.log.LogProtocol;
import dev.hotdb.cdc.port.CdcSource;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** {@link LogProtocol} 의 서버 쪽. 전달부가 당겨 간다 — 캡처부는 전달부를 부르지 않는다. */
@RestController
class ChangeLogController {

    private static final int MAX_RECORDS = 10_000;
    private static final long MAX_WAIT_MS = 5_000;

    private final FileChangeLog log;
    private final CdcSource source;

    ChangeLogController(FileChangeLog log, CdcSource source) {
        this.log = log;
        this.source = source;
    }

    @GetMapping(LogProtocol.INFO)
    LogProtocol.Info info() {
        return new LogProtocol.Info(log.firstSeq(), log.lastSeq(), source.capabilities(), log.acked(), log.bytes());
    }

    @GetMapping(value = LogProtocol.RECORDS, produces = MediaType.TEXT_PLAIN_VALUE)
    ResponseEntity<String> records(@RequestParam long from, @RequestParam(defaultValue = "2048") int max,
                                   @RequestParam(defaultValue = "1000") long waitMs) throws InterruptedException {
        List<String> lines;
        try {
            lines = log.read(from, Math.min(max, MAX_RECORDS), Math.min(waitMs, MAX_WAIT_MS));
        } catch (FileChangeLog.GoneException e) {
            return ResponseEntity.status(HttpStatus.GONE).body(e.getMessage());
        }
        StringBuilder body = new StringBuilder(lines.size() * 600);
        lines.forEach(l -> body.append(l).append('\n'));
        return ResponseEntity.ok()
                .header(LogProtocol.LAST_SEQ, Long.toString(log.lastSeq()))
                .header(LogProtocol.FIRST_SEQ, Long.toString(log.firstSeq()))
                .body(body.toString());
    }

    @PostMapping(LogProtocol.ACK)
    ResponseEntity<Void> ack(@RequestParam String consumer, @RequestParam long seq) {
        log.ack(consumer, seq);
        return ResponseEntity.noContent().build();
    }
}
