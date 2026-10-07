package dev.hotdb.cdc.adapter.debezium;

import dev.hotdb.cdc.HotdbCdcProperties;
import dev.hotdb.cdc.apply.DeadLetters;
import dev.hotdb.cdc.apply.PipelineHaltedException;
import dev.hotdb.cdc.continuity.CdcCheckpoints;
import dev.hotdb.cdc.continuity.SlotContinuityGuard;
import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.port.CdcSink;
import dev.hotdb.cdc.port.CdcSource;
import dev.hotdb.cdc.port.SourceCapabilities;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * {@link CdcSource} 의 Debezium 어댑터 — Debezium Embedded (Kafka 없음) 로 PostgreSQL 논리 복제 슬롯을 직접 읽는다.
 *
 * <p>배치 하나를 {@link CdcSink} 가 처리를 끝낸 뒤에야 오프셋을 넘긴다. 그 사이에 죽으면
 * 같은 배치가 다시 오는데(at-least-once) 싱크가 멱등이라 결과가 같다.
 * 오프셋은 파일({@code hotdb.cdc.offset-file}), 위치의 원본은 슬롯의 confirmed_flush 다.
 *
 * <p>이전 Java 판(embedded-cdc · latest-poc)의 DebeziumEngineManager 와 같은 안전장치를 둔다:
 * 기동 전 캡처 갭 검사, 배치마다 처리 위치 기록, 해석 실패는 dead letter, 정지 판정이면 프로세스를 살려 둔 채 멈춤,
 * 종료 때 엔진을 먼저 닫고 스레드를 기다림.
 */
public class DebeziumCdcSource implements CdcSource, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DebeziumCdcSource.class);
    static final SourceCapabilities CAPABILITIES = SourceCapabilities.logicalReplication("debezium");

    private final HotdbCdcProperties props;
    private final DebeziumEventDecoder decoder;
    private final CdcSink applier;
    private final DeadLetters deadLetters;
    private final CdcCheckpoints checkpoints;
    private final SlotContinuityGuard continuity;
    private final MeterRegistry meters;
    private final Timer batchTimer;
    private final DistributionSummary batchSize;
    private final Timer commitLag;
    private final EngineResources resources;
    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);
    private volatile Instant lastBatchAt;
    private volatile Instant lastEventCommitAt;
    private volatile String failure;
    private volatile boolean captureGap;
    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;

    public DebeziumCdcSource(HotdbCdcProperties props, DebeziumEventDecoder decoder, CdcSink applier,
                             DeadLetters deadLetters, CdcCheckpoints checkpoints, SlotContinuityGuard continuity,
                             MeterRegistry meters) {
        this.props = props;
        this.decoder = decoder;
        this.applier = applier;
        this.deadLetters = deadLetters;
        this.checkpoints = checkpoints;
        this.continuity = continuity;
        this.meters = meters;
        this.batchTimer = Timer.builder("hotdb.cdc.batch").description("배치 하나 해석 + 반영 + 커밋 시간")
                .publishPercentileHistogram().register(meters);
        this.batchSize = DistributionSummary.builder("hotdb.cdc.batch.size").register(meters);
        this.commitLag = Timer.builder("hotdb.cdc.lag.commit")
                .description("반영 끝 - 원천 커밋 시각(source.ts_ms). 서버 시계 편차가 섞인다")
                .publishPercentileHistogram().register(meters);
        this.resources = new EngineResources(props.pipeline(), meters);
        meters.gauge("hotdb.cdc.state", this, e -> e.state.get().ordinal());
        // 기동 때 판정이라 카운터로 두면 첫 수집 전에 1 이 되어 increase() 경보가 영영 안 걸린다 — 게이지로 둔다
        meters.gauge("hotdb.cdc.capture.gap", this, e -> e.captureGap ? 1 : 0);
        meters.gauge("hotdb.cdc.last.batch.age.seconds", this,
                e -> e.lastBatchAt == null ? -1 : Duration.between(e.lastBatchAt, Instant.now()).toMillis() / 1000.0);
    }

    @Override
    public void start() {
        if (!props.engineEnabled()) {
            log.warn("hotdb.cdc.engine-enabled=false — 엔진 없이 띄운다 (자원 측정 기준선용). 슬롯을 읽지 않는다");
            return;
        }
        // 되받을 수 없는 구간이 생겼는데 모르고 돌면 그 뒤로 계속 어긋난 채로 돈다. 재동기화는 사람이 판단할 일이다.
        // 예외로 컨텍스트를 죽이면 Prometheus 가 한 번도 못 긁으므로, 엔진만 띄우지 않고 앱은 살려 신호를 낸다.
        List<String> unsupported = applier.incompatibilities(CAPABILITIES);
        if (!unsupported.isEmpty()) {
            failure = "원천이 못 주는 것을 라우트가 쓴다: " + unsupported;
            state.set(State.HALTED);
            log.error("{}", failure);
            return;
        }
        Optional<String> gap = continuity.detectGap();
        captureGap = gap.isPresent();
        if (gap.isPresent()) {
            if (props.failOnCaptureGap()) {
                failure = "캡처 갭: " + gap.get();
                state.set(State.HALTED);
                log.error("캡처 연결고리 유실 — 엔진을 기동하지 않는다: {}", gap.get());
                return;
            }
            log.error("캡처 연결고리 유실 (fail-on-capture-gap=false 라 기동은 계속함): {}", gap.get());
        }

        state.set(State.STARTING);
        // 엔진을 그룹 스레드 위에서 만든다 — Debezium 이 생성자에서 만드는 스레드 풀도 그룹을 물려받게
        executor = Executors.newSingleThreadExecutor(r -> new Thread(resources.group(), r, "cdc-" + props.pipeline()));
        try {
            engine = executor.submit(this::buildEngine).get();
        } catch (ExecutionException e) {
            throw new IllegalStateException("CDC 엔진을 만들지 못했다", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("CDC 엔진 생성 중 인터럽트", e);
        }
        executor.execute(engine);
        log.info("CDC 엔진 시작: pipeline={} slot={} publication={}", props.pipeline(), props.slot(), props.publication());
    }

    private DebeziumEngine<ChangeEvent<String, String>> buildEngine() {
        return DebeziumEngine.create(Json.class)
                .using(debeziumProperties())
                .using((success, message, error) -> {
                    if (success) {
                        state.compareAndSet(State.RUNNING, State.STOPPED);
                        state.compareAndSet(State.STARTING, State.STOPPED);
                        return;
                    }
                    PipelineHaltedException halted = PipelineHaltedException.find(error);
                    if (halted != null) {
                        // 재시작해도 같은 이유로 멈춘다 — 끝내지 않고 health DOWN · hotdb_cdc_state=HALTED 로 알린다
                        failure = "정지 (" + halted.reason() + "): " + halted.getMessage();
                        state.set(State.HALTED);
                        log.error("CDC 정지 — 위치를 넘기지 않았다. 원인을 고친 뒤 재기동한다: {}", halted.getMessage());
                        return;
                    }
                    failure = message;
                    state.set(State.FAILED);
                    log.error("CDC 엔진 중단: {}", message, error);
                    if (props.exitOnFailure()) {
                        // 컨테이너 재시작에 맡긴다. 오프셋은 마지막 커밋 배치까지라 그 뒤부터 다시 받는다.
                        new Thread(() -> System.exit(1), "cdc-exit").start();
                    }
                })
                .notifying(this::handleBatch)
                .build();
    }

    private void handleBatch(List<ChangeEvent<String, String>> records,
                             DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        long[] usage = resources.startHandler();
        try {
            applyBatch(records, committer);
        } finally {
            resources.endHandler(usage);
        }
    }

    private void applyBatch(List<ChangeEvent<String, String>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        state.compareAndSet(State.STARTING, State.RUNNING);
        Timer.Sample sample = Timer.start(meters);
        List<CdcEvent> events = new ArrayList<>(records.size());
        Long maxLsn = null;
        for (ChangeEvent<String, String> r : records) {
            DebeziumEventDecoder.Decoded d;
            try {
                d = decoder.decode(r.value());
            } catch (RuntimeException ex) {
                // 깨진 레코드 하나가 엔진을 멈추지 않게 — 버리지 않고 원문째 남긴다. 원인을 고친 뒤 재처리한다
                log.error("레코드 해석 실패 — dead letter 로 격리: {}", ex.getMessage());
                deadLetters.storeUnparsable(r.value(), ex.getMessage());
                meters.counter("hotdb.cdc.events.ignored", "reason", "unparsable").increment();
                meters.counter("hotdb.cdc.dead.letter", "route", DeadLetters.UNPARSABLE).increment();
                continue;
            }
            if (d.event() == null) {
                meters.counter("hotdb.cdc.events.ignored", "reason", d.skipReason()).increment();
                continue;
            }
            CdcEvent e = d.event();
            if (e.lsn() != null && (maxLsn == null || e.lsn() > maxLsn)) {
                maxLsn = e.lsn();   // 넘기지 않는 표(heartbeat 등)도 위치는 전진한다
            }
            meters.counter("hotdb.cdc.events", "source", e.table().toString(), "op", e.op().code()).increment();
            if (!applier.interestedIn(e.table())) {
                meters.counter("hotdb.cdc.events.ignored", "reason", "no_route").increment();
                continue;
            }
            events.add(e);
        }
        applier.apply(events);
        for (ChangeEvent<String, String> r : records) {
            committer.markProcessed(r);
        }
        committer.markBatchFinished();
        checkpoints.record(maxLsn, true);

        Instant now = Instant.now();
        lastBatchAt = now;
        for (CdcEvent e : events) {
            if (e.commitTsMs() != null) {
                commitLag.record(Duration.between(Instant.ofEpochMilli(e.commitTsMs()), now));
                lastEventCommitAt = Instant.ofEpochMilli(e.commitTsMs());
            }
        }
        batchSize.record(records.size());
        sample.stop(batchTimer);
    }

    Properties debeziumProperties() {
        HotdbCdcProperties.Source s = props.source();
        Properties p = new Properties();
        p.setProperty("name", props.pipeline());
        p.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        p.setProperty("topic.prefix", props.pipeline());
        p.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        p.setProperty("offset.storage.file.filename", props.offsetFile());
        p.setProperty("offset.flush.interval.ms", "1000");
        p.setProperty("database.hostname", s.host());
        p.setProperty("database.port", String.valueOf(s.port()));
        p.setProperty("database.user", s.user());
        p.setProperty("database.password", s.password());
        p.setProperty("database.dbname", s.database());
        p.setProperty("plugin.name", "pgoutput");
        p.setProperty("slot.name", props.slot());
        p.setProperty("publication.name", props.publication());
        // publication 은 마이그레이션(V4)이 만든다. 소비자가 고치면 다른 소비자 몫까지 바뀐다
        p.setProperty("publication.autocreate.mode", "disabled");
        p.setProperty("table.include.list", String.join(",", props.includeTables()));
        p.setProperty("snapshot.mode", props.snapshotMode());
        p.setProperty("heartbeat.interval.ms", String.valueOf(props.heartbeatIntervalMs()));
        p.setProperty("heartbeat.action.query", "INSERT INTO ops.cdc_heartbeat (pipeline, beat_at) VALUES ('"
                + props.pipeline().replace("'", "''") + "', now()) ON CONFLICT (pipeline) DO UPDATE SET beat_at = now()");
        p.setProperty("max.batch.size", String.valueOf(props.maxBatchSize()));
        p.setProperty("max.queue.size", String.valueOf(props.maxQueueSize()));
        p.setProperty("poll.interval.ms", String.valueOf(props.pollIntervalMs()));
        p.setProperty("decimal.handling.mode", "double");
        p.setProperty("tombstones.on.delete", "false");
        p.setProperty("converter.schemas.enable", "false");
        props.debezium().forEach(p::setProperty);
        return p;
    }

    /**
     * 엔진을 먼저 닫고(남은 배치 전달 + 오프셋 기록) 스레드를 기다린다 — executor 를 먼저 내리면
     * 스레드가 인터럽트되어 오프셋 기록이 깨질 수 있다 (Debezium 문서 경고). 닫은 뒤 마지막 확인 위치를 남긴다.
     */
    @Override
    public void stop() {
        if (engine != null) {
            try {
                engine.close();
            } catch (IOException e) {
                log.warn("CDC 엔진 종료 중 오류", e);
            }
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("CDC 엔진 스레드가 30초 안에 끝나지 않았다");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (engine != null && state.get() != State.HALTED) {
            try {
                checkpoints.record(null, false);
            } catch (RuntimeException e) {
                log.warn("종료 때 처리 위치를 남기지 못했다 (다음 배치에서 다시 남긴다): {}", e.getMessage());
            }
        }
        if (state.get() != State.HALTED) {
            state.set(State.STOPPED);
        }
    }

    @Override
    public Optional<CdcEvent> decode(String raw) {
        return Optional.ofNullable(decoder.decode(raw).event());
    }

    @Override
    public boolean isRunning() {
        return status().healthy();
    }

    @Override
    public int getPhase() {
        // DataSource 등 다른 빈이 다 뜬 뒤에 시작하고, 먼저 멈춘다
        return Integer.MAX_VALUE - 100;
    }

    @Override
    public SourceCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public Status status() {
        return new Status(state.get(), props.pipeline(), props.slot(), lastBatchAt, lastEventCommitAt, failure);
    }
}
