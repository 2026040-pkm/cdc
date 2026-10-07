package dev.hotdb.sim;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** 장비 등록(시드) + status · actual · artifact 주기 적재. */
@Component
public class FieldSimulator {

    private static final Logger log = LoggerFactory.getLogger(FieldSimulator.class);
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    /** .NET 7자리 원문 흉내 (source_time_text) */
    private static final DateTimeFormatter DOTNET = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSXXX");
    private static final Map<String, String> PREFIX = Map.of(
            "ASSEMBLY", "ASM", "OUTFITTING", "OFT", "PAINTING", "PNT", "MACHINING", "MCH");
    private static final String[] TYPES = {"REGISTERED_PCD", "TRANSFORMATION_MATRIX", "SEGMENTED_PCD"};

    private final SimProperties props;
    private final JdbcTemplate jdbc;
    private final MeterRegistry meters;
    private final List<Device> devices = new ArrayList<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final Map<String, AtomicLong> rows = new LinkedHashMap<>();
    private volatile double speed;
    private volatile boolean running;
    private ScheduledFuture<?> statusTask;
    private ScheduledFuture<?> actualTask;

    /** 장비 한 대와 그 장비의 태그 · 진행 중 스캔. */
    static final class Device {
        final String zone;
        final String deviceId;
        long statusTag;
        long actualTag;
        final Map<String, Long> artifactTag = new LinkedHashMap<>();
        UUID scanId;
        double progress;
        String hull;
        String block;

        Device(String zone, String deviceId) {
            this.zone = zone;
            this.deviceId = deviceId;
        }
    }

    public FieldSimulator(SimProperties props, JdbcTemplate jdbc, MeterRegistry meters) {
        this.props = props;
        this.jdbc = jdbc;
        this.meters = meters;
        this.speed = props.speed();
        for (String ch : List.of("status", "actual", "artifact")) {
            AtomicLong c = new AtomicLong();
            rows.put(ch, c);
            meters.more().counter("hotdb.sim.rows", io.micrometer.core.instrument.Tags.of("channel", ch), c);
            meters.counter("hotdb.sim.errors", "channel", ch);
        }
        meters.gauge("hotdb.sim.speed", this, s -> s.speed);
        meters.gauge("hotdb.sim.running", this, s -> s.running ? 1 : 0);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        seed();
        if (props.autoStart()) {
            resume();
        }
    }

    // ── 시드: 장비 · 태그 등록 (HotDB Provider 의 태그 등록부 대신) ────────────────────

    void seed() {
        props.devices().forEach((zone, count) -> {
            String p = PREFIX.getOrDefault(zone, zone.substring(0, 3));
            for (int i = 1; i <= count; i++) {
                devices.add(new Device(zone, String.format("LDR-%s-%03d", p, i)));
            }
        });
        String topicZone;
        for (Device d : devices) {
            topicZone = d.zone.toLowerCase();
            int shop = 1 + (devices.indexOf(d) % 3);
            jdbc.update("""
                    INSERT INTO tsdb.device (site, device_id, device_role, zone, shop, bay)
                    VALUES (?, ?, 'LIDAR', ?, ?, ?) ON CONFLICT DO NOTHING
                    """, props.site(), d.deviceId, d.zone, topicZone + shop, "bay" + (1 + devices.indexOf(d) % 7));
            d.statusTag = tag(d, "status", "ot/device/" + topicZone + "/status", d.deviceId, null);
            d.actualTag = tag(d, "actual", "ot/sensor/" + topicZone + "/actual", d.deviceId, null);
            for (String t : TYPES) {
                d.artifactTag.put(t, tag(d, "artifact", "ot/pipeline/" + topicZone + "/artifact", d.deviceId + "-" + t, t));
            }
        }
        log.info("시드 완료: 장비 {}대 · 태그 {}개 ({})", devices.size(), devices.size() * 5, props.devices());
    }

    private long tag(Device d, String channel, String topic, String sourceId, String artifactType) {
        String tagId = sourceId + "." + topic.replace('/', '_') + ".raw_payload";
        jdbc.update("""
                INSERT INTO tsdb.tag_catalog (edge_group_id, tag_id, mqtt_topic, source_id, site, device_id, channel, artifact_type)
                VALUES ('sim', ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (edge_group_id, tag_id) DO NOTHING
                """, tagId, topic, sourceId, props.site(), d.deviceId, channel, artifactType);
        return jdbc.queryForObject("SELECT tag_key FROM tsdb.tag_catalog WHERE edge_group_id = 'sim' AND tag_id = ?",
                Long.class, tagId);
    }

    // ── 제어 ───────────────────────────────────────────────────────────────

    public synchronized void resume() {
        if (running) {
            return;
        }
        running = true;
        statusTask = scheduler.scheduleAtFixedRate(guard("status", this::tickStatus), 0,
                Math.max(10, (long) (props.statusIntervalMs() / speed)), TimeUnit.MILLISECONDS);
        actualTask = scheduler.scheduleAtFixedRate(guard("actual", this::tickActual), 0,
                Math.max(10, (long) (props.actualIntervalMs() / speed)), TimeUnit.MILLISECONDS);
        log.info("발행 시작: 배속 x{} (status {}ms · actual {}ms)", speed,
                (long) (props.statusIntervalMs() / speed), (long) (props.actualIntervalMs() / speed));
    }

    public synchronized void pause() {
        running = false;
        if (statusTask != null) {
            statusTask.cancel(false);
        }
        if (actualTask != null) {
            actualTask.cancel(false);
        }
        log.info("발행 멈춤");
    }

    public synchronized void speed(double value) {
        if (value <= 0) {
            throw new IllegalArgumentException("배속은 0 보다 커야 합니다");
        }
        boolean was = running;
        pause();
        speed = value;
        if (was) {
            resume();
        }
    }

    public Map<String, Object> status() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("running", running);
        out.put("speed", speed);
        out.put("devices", props.devices());
        double perSec = devices.size() * (1000.0 / props.statusIntervalMs()
                + (1 + (props.artifacts() ? 12 : 0)) * 1000.0 / props.actualIntervalMs()) * speed;
        out.put("expectedRowsPerSec", Math.round(perSec));
        Map<String, Long> totals = new LinkedHashMap<>();
        rows.forEach((k, v) -> totals.put(k, v.get()));
        out.put("rowsTotal", totals);
        return out;
    }

    private Runnable guard(String name, Runnable r) {
        Timer timer = Timer.builder("hotdb.sim.tick").tag("channel", name).publishPercentileHistogram().register(meters);
        return () -> {
            try {
                timer.record(r);
            } catch (RuntimeException e) {
                // 예외가 나가면 scheduleAtFixedRate 가 조용히 멈춘다 — 기록만 하고 다음 주기에 다시 한다
                meters.counter("hotdb.sim.errors", "channel", name).increment();
                log.warn("{} 적재 실패: {}", name, e.getMessage());
            }
        };
    }

    // ── status: 장비마다 1행 ──────────────────────────────────────────────────

    void tickStatus() {
        OffsetDateTime now = OffsetDateTime.now(KST);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        jdbc.batchUpdate("""
                INSERT INTO tsdb.status_history (event_time, tag_key, status, error_code, last_heartbeat_at,
                  scan_rate_pts_per_sec, temperature_c, connectivity_rssi, fov_mode,
                  source_time_text, source_ingested_at, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, clock_timestamp())
                ON CONFLICT DO NOTHING
                """, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Device d = devices.get(i);
                double roll = rnd.nextDouble();
                String status = roll < props.anomalyRate() / 2 ? "ERROR" : roll < props.anomalyRate() ? "CALIBRATING" : "ONLINE";
                ps.setObject(1, now);
                ps.setLong(2, d.statusTag);
                ps.setString(3, status);
                ps.setString(4, "ERROR".equals(status) ? "E" + (100 + rnd.nextInt(5)) : null);
                ps.setObject(5, now.minusNanos(7_000_000));
                ps.setInt(6, 200_000 + rnd.nextInt(60_000));
                ps.setFloat(7, (float) (38 + rnd.nextDouble() * 6));
                ps.setShort(8, (short) (-40 - rnd.nextInt(30)));
                ps.setString(9, "wide");
                ps.setString(10, now.format(DOTNET));
                ps.setObject(11, now.plusNanos(1_170_000_000L));
            }

            @Override
            public int getBatchSize() {
                return devices.size();
            }
        });
        rows.get("status").addAndGet(devices.size());
    }

    // ── actual + artifact: 장비마다 1행 + 산출물 12행 ────────────────────────────

    void tickActual() {
        OffsetDateTime now = OffsetDateTime.now(KST);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        List<Object[]> actual = new ArrayList<>(devices.size());
        List<Device> forArtifacts = new ArrayList<>(devices.size());
        for (Device d : devices) {
            String type;
            if (d.scanId == null) {
                d.scanId = UUID.randomUUID();
                d.progress = 0;
                d.hull = "H" + (1200 + rnd.nextInt(20));
                d.block = "B" + (100 + rnd.nextInt(80)) + (rnd.nextBoolean() ? "P" : "S");
                type = "START";
            } else {
                d.progress = Math.min(100, d.progress + 10 + rnd.nextDouble() * 25);
                type = d.progress >= 100 ? "COMPLETE" : "PROGRESS";
            }
            actual.add(new Object[]{now, d.actualTag, d.scanId, type, d.hull, d.block, now,
                    (float) d.progress, (float) (0.85 + rnd.nextDouble() * 0.14),
                    "CAD-" + d.hull + "-" + d.block, "seg-v1.4.2", now.format(DOTNET), now.plusNanos(930_000_000L)});
            forArtifacts.add(d);
        }
        jdbc.batchUpdate("""
                INSERT INTO tsdb.actual_history (event_time, tag_key, scan_id, event_type, hull_no, block_id, scanned_at,
                  block_progress_rate, match_confidence, reference_cad_id, model_version,
                  source_time_text, source_ingested_at, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, clock_timestamp())
                ON CONFLICT DO NOTHING
                """, actual);
        rows.get("actual").addAndGet(actual.size());

        if (props.artifacts()) {
            insertArtifacts(now, forArtifacts);
        }
        // COMPLETE 를 낸 스캔은 닫는다 — 다음 주기에 새 스캔 START
        for (Device d : devices) {
            if (d.progress >= 100) {
                d.scanId = null;
            }
        }
    }

    private void insertArtifacts(OffsetDateTime now, List<Device> ds) {
        List<Object[]> out = new ArrayList<>(ds.size() * 12);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (Device d : ds) {
            String base = "file://ot-" + d.zone.toLowerCase() + "/pcd/" + now.toLocalDate() + "/" + d.scanId;
            out.add(artifact(now, d, "REGISTERED_PCD", "", base + "/registered.pcd", 150_000_000L + rnd.nextInt(50_000_000), null));
            out.add(artifact(now, d, "TRANSFORMATION_MATRIX", "", null, null, matrix(rnd)));
            for (int s = 1; s <= 10; s++) {
                String seg = String.format("SEG-%02d", s);
                out.add(artifact(now, d, "SEGMENTED_PCD", seg, base + "/" + seg + ".pcd", 10_000_000L + rnd.nextInt(5_000_000), null));
            }
        }
        jdbc.batchUpdate("""
                INSERT INTO tsdb.artifact_history (event_time, tag_key, scan_id, artifact_type, segment_key, hull_no, block_id,
                  storage_uri, file_size_bytes, checksum, transformation_matrix, produced_by_device_id, model_version,
                  source_time_text, source_ingested_at, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS real[]), ?, ?, ?, ?, clock_timestamp())
                ON CONFLICT DO NOTHING
                """, out);
        rows.get("artifact").addAndGet(out.size());
    }

    private Object[] artifact(OffsetDateTime now, Device d, String type, String seg, String uri, Long size, String matrix) {
        return new Object[]{now, d.artifactTag.get(type), d.scanId, type, seg, d.hull, d.block, uri, size,
                uri == null ? null : "sha256:" + Long.toHexString(ThreadLocalRandom.current().nextLong()),
                matrix, "INF-" + PREFIX.getOrDefault(d.zone, "XXX") + "-01", "seg-v1.4.2",
                now.format(DOTNET), now.plusNanos(670_000_000L)};
    }

    private static String matrix(ThreadLocalRandom rnd) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < 16; i++) {
            sb.append(i == 0 ? "" : ",").append(i % 5 == 0 ? "1.0" : String.format("%.4f", rnd.nextDouble() * 0.1));
        }
        return sb.append('}').toString();
    }
}
