package dev.hotdb.cdc.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hotdb.cdc.HotdbCdcProperties;
import dev.hotdb.cdc.apply.PipelineHaltedException;
import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.port.CdcSink;
import dev.hotdb.cdc.port.CdcSource;
import dev.hotdb.cdc.port.SourceCapabilities;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * {@link CdcSource} 의 log 어댑터 — 원천 DB 가 아니라 캡처부(capture-service)의 변경 로그를 당겨 온다.
 *
 * <p>전달부는 엔진도 슬롯도 모른다. 아는 것은 순번(seq) 하나와 {@link ChangeRecord} 형식뿐이다.
 * 위치(오프셋)는 이 프로세스가 혼자 책임진다: 배치를 {@link CdcSink} 가 끝낸 뒤 파일에 남기고, 캡처부에 ack 한다.
 * ack 는 캡처부 로그 보존의 기준일 뿐이라 실패해도 반영은 계속한다 — 다음 ack 가 덮는다.
 *
 * <p>캡처부에 못 붙으면 기다린다(STARTING, health UP). 전달부 자신이 아픈 것이 아니고, 붙으면 남긴 순번부터 잇는다.
 * 남긴 순번이 캡처부 로그에서 이미 지워졌으면(410) 되받을 수 없는 구간이라 멈춘다(HALTED) — 일체형의 캡처 갭과 같다.
 */
public class LogPullSource implements CdcSource, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(LogPullSource.class);

    private final HotdbCdcProperties cdc;
    private final LogProperties props;
    private final CdcSink sink;
    private final ChangeRecordCodec codec;
    private final ObjectMapper json;
    private final MeterRegistry meters;
    private final FileOffset offset;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);
    private final AtomicLong applied = new AtomicLong(-1);
    private final AtomicLong upstreamLast = new AtomicLong(-1);
    private final Timer batchTimer;
    private final DistributionSummary batchSize;
    private final Timer commitLag;
    private volatile SourceCapabilities capabilities;
    private volatile Instant lastBatchAt;
    private volatile Instant lastEventCommitAt;
    private volatile String failure;
    private volatile boolean running;
    private Thread worker;

    public LogPullSource(HotdbCdcProperties cdc, LogProperties props, CdcSink sink, ObjectMapper json,
                         MeterRegistry meters) {
        this.cdc = cdc;
        this.props = props;
        this.sink = sink;
        this.json = json;
        this.codec = new ChangeRecordCodec(json);
        this.meters = meters;
        this.offset = new FileOffset(Path.of(props.offsetFile()));
        // 지표 이름은 Debezium 어댑터와 같다 — 대시보드가 원천을 가리지 않게
        this.batchTimer = Timer.builder("hotdb.cdc.batch").description("배치 하나 해석 + 반영 + 커밋 시간")
                .publishPercentileHistogram().register(meters);
        this.batchSize = DistributionSummary.builder("hotdb.cdc.batch.size").register(meters);
        this.commitLag = Timer.builder("hotdb.cdc.lag.commit")
                .description("반영 끝 - 원천 커밋 시각(source.ts_ms). 서버 시계 편차가 섞인다")
                .publishPercentileHistogram().register(meters);
        meters.gauge("hotdb.cdc.state", this, e -> e.state.get().ordinal());
        meters.gauge("hotdb.cdc.last.batch.age.seconds", this,
                e -> e.lastBatchAt == null ? -1 : Duration.between(e.lastBatchAt, Instant.now()).toMillis() / 1000.0);
        meters.gauge("hotdb.log.applied.seq", applied);
        meters.gauge("hotdb.log.behind.records", this,
                e -> e.upstreamLast.get() < 0 || e.applied.get() < 0 ? Double.NaN : e.upstreamLast.get() - e.applied.get());
        meters.counter("hotdb.log.pull.errors");
        meters.counter("hotdb.log.ack.errors");
    }

    @Override
    public void start() {
        running = true;
        state.set(State.STARTING);
        worker = new Thread(this::loop, "log-pull-" + cdc.pipeline());
        worker.start();
        log.info("변경 로그 당겨오기 시작: pipeline={} capture={} consumer={}", cdc.pipeline(), props.captureUrl(), consumer());
    }

    private void loop() {
        long next = -1;
        while (running) {
            try {
                if (next < 0) {
                    next = handshake();
                    if (next < 0) {
                        return;   // 멈춤 판정
                    }
                }
                HttpResponse<String> res = get(LogProtocol.RECORDS + "?from=" + next + "&max=" + props.maxRecords()
                        + "&waitMs=" + props.waitMs(), Duration.ofMillis(props.waitMs() + 10_000));
                if (res.statusCode() == 410) {
                    halt("캡처 로그에서 이미 지워진 구간을 요청했다 (from=" + next + "): " + res.body());
                    return;
                }
                if (res.statusCode() != 200) {
                    throw new IOException("capture " + res.statusCode() + ": " + res.body());
                }
                res.headers().firstValueAsLong(LogProtocol.LAST_SEQ).ifPresent(upstreamLast::set);
                failure = null;
                state.compareAndSet(State.STARTING, State.RUNNING);
                String body = res.body();
                if (!body.isEmpty()) {
                    next = applyBatch(body) + 1;
                }
            } catch (PipelineHaltedException e) {
                halt("정지 (" + e.reason() + "): " + e.getMessage());
                return;
            } catch (IOException e) {
                try {
                    waitForCapture(e);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                failure = e.getMessage();
                state.set(State.FAILED);
                log.error("반영 실패 — 위치를 넘기지 않았다", e);
                if (cdc.exitOnFailure()) {
                    new Thread(() -> System.exit(1), "cdc-exit").start();   // 컨테이너 재시작 → 남긴 순번부터 다시
                }
                return;
            }
        }
    }

    /** 캡처부에 처음 붙을 때 — 원천이 주는 것을 라우트와 맞춰 보고, 어디서부터 받을지 정한다. -1 = 멈춤 */
    private long handshake() throws IOException, InterruptedException {
        HttpResponse<String> res = get(LogProtocol.INFO, Duration.ofSeconds(5));
        if (res.statusCode() != 200) {
            throw new IOException("capture " + res.statusCode() + ": " + res.body());
        }
        LogProtocol.Info info = json.readValue(res.body(), LogProtocol.Info.class);
        capabilities = info.source();
        List<String> unsupported = sink.incompatibilities(info.source());
        if (!unsupported.isEmpty()) {
            halt("원천(" + info.source().adapter() + ")이 못 주는 것을 라우트가 쓴다: " + unsupported);
            return -1;
        }
        OptionalLong saved = offset.read();
        long from = saved.isPresent() ? saved.getAsLong() + 1 : Math.max(1, info.firstSeq());
        if (saved.isPresent()) {
            applied.set(saved.getAsLong());
        }
        if (from < info.firstSeq()) {
            halt("남긴 순번 " + saved.getAsLong() + " 다음이 캡처 로그에 없다 (남은 가장 앞 " + info.firstSeq() + ")");
            return -1;
        }
        log.info("캡처부 연결: 원천={} 위치종류={} 로그 {}~{} · {} 부터 받는다", info.source().adapter(),
                info.source().positionKind(), info.firstSeq(), info.lastSeq(), from);
        return from;
    }

    /** @return 이 배치의 마지막 순번 */
    private long applyBatch(String body) throws InterruptedException {
        Timer.Sample sample = Timer.start(meters);
        String[] lines = body.split("\n");
        List<CdcEvent> events = new ArrayList<>(lines.length);
        long last = -1;
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            ChangeRecord r = codec.decodeLine(line);
            if (last >= 0 && r.seq() != last + 1) {
                throw new IllegalStateException("순번이 건너뛰었다: " + last + " 다음 " + r.seq());
            }
            last = r.seq();
            CdcEvent e = r.event();
            meters.counter("hotdb.cdc.events", "source", e.table().toString(), "op", e.op().code()).increment();
            if (!sink.interestedIn(e.table())) {
                meters.counter("hotdb.cdc.events.ignored", "reason", "no_route").increment();
                continue;
            }
            events.add(e);
        }
        sink.apply(events);
        offset.write(last);
        applied.set(last);
        ack(last);

        Instant now = Instant.now();
        lastBatchAt = now;
        for (CdcEvent e : events) {
            if (e.commitTsMs() != null) {
                commitLag.record(Duration.between(Instant.ofEpochMilli(e.commitTsMs()), now));
                lastEventCommitAt = Instant.ofEpochMilli(e.commitTsMs());
            }
        }
        batchSize.record(lines.length);
        sample.stop(batchTimer);
        return last;
    }

    private void ack(long seq) throws InterruptedException {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(props.captureUrl() + LogProtocol.ACK + "?consumer="
                    + consumer() + "&seq=" + seq)).timeout(Duration.ofSeconds(3)).POST(HttpRequest.BodyPublishers.noBody()).build();
            http.send(req, HttpResponse.BodyHandlers.discarding());
        } catch (IOException e) {
            meters.counter("hotdb.log.ack.errors").increment();   // 보존 기준만 늦어진다. 다음 배치의 ack 가 덮는다
        }
    }

    private void waitForCapture(IOException e) throws InterruptedException {
        meters.counter("hotdb.log.pull.errors").increment();
        if (failure == null) {
            log.warn("캡처부에 못 붙는다 — {}ms 마다 다시 (반영한 순번 {} 에서 잇는다): {}", props.retryMs(), applied.get(),
                    e.toString());
        }
        failure = "캡처부 연결 안 됨: " + e.getMessage();
        state.set(State.STARTING);
        Thread.sleep(props.retryMs());
    }

    private void halt(String why) {
        failure = why;
        state.set(State.HALTED);
        log.error("변경 로그 반영 정지 — 위치를 넘기지 않았다. 원인을 고친 뒤 재기동한다: {}", why);
    }

    private HttpResponse<String> get(String pathAndQuery, Duration timeout) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(props.captureUrl() + pathAndQuery)).timeout(timeout).GET().build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private String consumer() {
        return props.consumer() == null || props.consumer().isBlank() ? cdc.pipeline() : props.consumer();
    }

    @Override
    public void stop() {
        // 끊지 않고 기다린다 — 반영 트랜잭션 도중에 인터럽트하면 JDBC 가 깨진다. long poll 은 waitMs 안에 돌아온다
        running = false;
        if (worker != null) {
            try {
                worker.join(30_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (state.get() != State.HALTED) {
            state.set(State.STOPPED);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    @Override
    public Status status() {
        return new Status(state.get(), cdc.pipeline(), "log-seq:" + applied.get(), lastBatchAt, lastEventCommitAt, failure);
    }

    @Override
    public SourceCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public Optional<CdcEvent> decode(String raw) {
        return Optional.of(codec.decodeJson(raw));
    }
}
