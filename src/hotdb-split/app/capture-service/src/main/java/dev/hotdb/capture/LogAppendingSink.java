package dev.hotdb.capture;

import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.log.ChangeRecordCodec;
import dev.hotdb.cdc.port.CdcSink;
import java.util.ArrayList;
import java.util.List;

/**
 * 캡처부의 {@link CdcSink} — 표에 쓰지 않고 변경 로그에 덧붙인다. 돌아오면(= fsync 뒤) 엔진이 슬롯 위치를 넘긴다.
 *
 * <p>무엇을 받을지 고르지 않는다(전부 받는다). 거르는 일은 라우트를 가진 전달부 몫이다 — 그래서 전달부를 여럿 붙여도
 * 캡처부와 슬롯은 하나면 된다.
 */
final class LogAppendingSink implements CdcSink {

    private final FileChangeLog log;
    private final ChangeRecordCodec codec;

    LogAppendingSink(FileChangeLog log, ChangeRecordCodec codec) {
        this.log = log;
        this.codec = codec;
    }

    @Override
    public boolean interestedIn(TableId table) {
        return true;
    }

    @Override
    public void apply(List<CdcEvent> events) {
        List<String> bodies = new ArrayList<>(events.size());
        for (CdcEvent e : events) {
            bodies.add(codec.encode(e.lsn() == null ? null : "lsn:" + e.lsn(), e));
        }
        try {
            log.append(bodies);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("변경 로그 덧붙이기 중 인터럽트 — 위치를 넘기지 않는다", ex);
        }
    }
}
