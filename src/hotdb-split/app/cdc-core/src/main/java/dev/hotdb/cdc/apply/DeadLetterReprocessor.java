package dev.hotdb.cdc.apply;

import dev.hotdb.cdc.HotdbCdcProperties;
import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.port.CdcSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * 격리된 이벤트를 다시 반영한다 (이전 Java 판 embedded-cdc 의 DeadLetterReprocessor 그대로).
 *
 * <p><b>PENDING 을 자동으로 집지 않는다.</b> 원인이 고쳐졌는지는 사람만 안다 — 자동으로 돌리면 고쳐지지 않은
 * 건이 영원히 재시도되며 잡음만 쌓인다. 원인을 고친 뒤 상태를 바꾸는 것이 재처리 신청이다:
 * <pre>UPDATE ops.cdc_dead_letter SET status = 'RETRY_REQUESTED' WHERE id = 1;</pre>
 *
 * <p>재처리가 안전한 이유는 라우트의 newer-than · ON CONFLICT 에 있다. 격리된 뒤 같은 행에 더 새 값이 이미
 * 들어왔으면 0행으로 막힌다 — STALE_SKIPPED 로 남긴다. "반영했다"(APPLIED)와 갈라 둬야 운영 화면에서 구분된다.
 */
public class DeadLetterReprocessor implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReprocessor.class);

    private final DeadLetters deadLetters;
    private final RouteApplier applier;
    private final CdcSource source;
    private final HotdbCdcProperties.DeadLetter props;
    private final MeterRegistry meters;
    private ScheduledExecutorService scheduler;

    public DeadLetterReprocessor(DeadLetters deadLetters, RouteApplier applier, CdcSource source,
                                 HotdbCdcProperties.DeadLetter props, MeterRegistry meters) {
        this.deadLetters = deadLetters;
        this.applier = applier;
        this.source = source;
        this.props = props;
        this.meters = meters;
        for (String outcome : List.of(DeadLetters.APPLIED, DeadLetters.STALE_SKIPPED, "FAILED")) {
            meters.counter("hotdb.cdc.dead.letter.reprocessed", "outcome", outcome);
        }
    }

    /** 신청된 건을 한 번 훑는다. 테스트와 스케줄러가 부른다. */
    public void reprocessOnce() {
        List<DeadLetters.Pending> claimed = deadLetters.claimRequested(props.reprocessBatchSize());
        if (claimed.isEmpty()) {
            return;
        }
        int applied = 0;
        int skipped = 0;
        int failed = 0;
        for (DeadLetters.Pending p : claimed) {
            try {
                Optional<CdcEvent> event = source.decode(p.raw());
                if (event.isEmpty()) {
                    throw new IllegalStateException("원문이 변경 레코드가 아니다");
                }
                int rows = applier.reapply(p.route(), event.get());
                String outcome = rows > 0 ? DeadLetters.APPLIED : DeadLetters.STALE_SKIPPED;
                deadLetters.resolved(p.id(), outcome);
                meters.counter("hotdb.cdc.dead.letter.reprocessed", "outcome", outcome).increment();
                if (rows > 0) {
                    applied++;
                } else {
                    skipped++;
                }
                log.info("재처리 {} id={} route={} 행={}", outcome, p.id(), p.route(), rows);
            } catch (RuntimeException e) {
                deadLetters.retryFailed(p.id(), rootMessage(e));
                meters.counter("hotdb.cdc.dead.letter.reprocessed", "outcome", "FAILED").increment();
                failed++;
                log.warn("재처리 실패 id={} route={} — PENDING 으로 되돌림: {}", p.id(), p.route(), rootMessage(e));
            }
        }
        log.info("dead letter 재처리 — 반영 {} · 차단 {} · 실패 {}", applied, skipped, failed);
    }

    @Override
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "dead-letter-reprocess"));
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                reprocessOnce();
            } catch (RuntimeException e) {
                // DB 가 잠깐 안 될 때 — 다음 주기에 다시 본다
                log.warn("dead letter 재처리 주기 실패: {}", rootMessage(e));
            }
        }, props.reprocessIntervalMs(), props.reprocessIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    @Override
    public boolean isRunning() {
        return scheduler != null;
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getMessage();
    }
}
