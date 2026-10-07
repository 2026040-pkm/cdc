package dev.hotdb.capture;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 캡처부의 변경 로그 — 덧붙이기만 하는 파일 묶음(세그먼트). 한 줄 = {@code seq\tjson\n}.
 *
 * <p>캡처부는 배치를 여기 쓰고 fsync 한 뒤에야 슬롯 위치를 넘긴다. 그래서 슬롯이 넘어간 변경은 반드시 이 로그에 있다.
 * 지우는 기준은 등록된 소비자(전달부) 전부가 ack 한 순번이다 — 멈춘 전달부가 있으면 그만큼 남는다.
 * 로그가 {@code maxBytes} 를 넘으면 덧붙이기를 막는다 → 엔진 큐가 차고 → 슬롯 읽기가 멈춰 WAL 이 Hot DB 에 남는다.
 * 일체형과 같은 마지막 방어선(슬롯)으로 돌아가는 것이고, 그 전까지는 적체를 Hot DB 가 아니라 캡처부 디스크가 진다.
 *
 * <p>세그먼트 이름은 첫 순번(20자리). 읽기는 세그먼트마다 128줄 간격 색인으로 시작 위치를 찾는다.
 * 기동 때 마지막 세그먼트 끝의 반쯤 쓴 줄(죽은 순간)은 잘라 낸다 — 그 줄은 fsync 전이라 슬롯도 넘어가지 않았다.
 */
final class FileChangeLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FileChangeLog.class);
    private static final int INDEX_EVERY = 128;

    private final Path dir;
    private final long segmentBytes;
    private final long maxBytes;
    private final List<String> consumers;
    private final Timer appendTimer;
    /** 첫 순번 → 세그먼트 */
    private final ConcurrentSkipListMap<Long, Segment> segments = new ConcurrentSkipListMap<>();
    private final Map<String, Long> acked = new java.util.concurrent.ConcurrentHashMap<>();
    private final Path ackFile;
    private FileChannel active;
    private long lastSeq;
    private volatile boolean blocked;

    private static final class Segment {
        final long firstSeq;
        final Path path;
        /** 순번 → 줄 시작 바이트 (INDEX_EVERY 줄마다) */
        final TreeMap<Long, Long> index = new TreeMap<>();
        volatile long lastSeq;
        /** fsync 까지 끝난 길이 — 읽기는 여기까지만 */
        volatile long committedBytes;

        Segment(long firstSeq, Path path) {
            this.firstSeq = firstSeq;
            this.path = path;
            this.lastSeq = firstSeq - 1;
        }
    }

    FileChangeLog(Path dir, long segmentBytes, long maxBytes, Collection<String> consumers, MeterRegistry meters)
            throws IOException {
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.maxBytes = maxBytes;
        this.consumers = List.copyOf(consumers);
        this.ackFile = dir.resolve("acks.properties");
        Files.createDirectories(dir);
        recover();
        this.appendTimer = Timer.builder("hotdb.log.append").description("배치 하나 쓰기 + fsync")
                .publishPercentileHistogram().register(meters);
        Gauge.builder("hotdb.log.last.seq", this, l -> l.lastSeq()).register(meters);
        Gauge.builder("hotdb.log.first.seq", this, l -> l.firstSeq()).register(meters);
        Gauge.builder("hotdb.log.bytes", this, l -> l.bytes()).baseUnit("bytes").register(meters);
        Gauge.builder("hotdb.log.segments", segments, Map::size).register(meters);
        Gauge.builder("hotdb.log.blocked", this, l -> l.blocked ? 1 : 0)
                .description("로그가 가득 차 덧붙이기를 막고 있다 → 슬롯이 WAL 을 쥔다").register(meters);
        for (String c : this.consumers) {
            Gauge.builder("hotdb.log.acked.seq", this, l -> l.ackedOf(c)).tag("consumer", c).register(meters);
        }
    }

    // ── 쓰기 ────────────────────────────────────────────────────────────────

    /** 배치를 덧붙이고 fsync. 돌아오면 디스크에 있다. @return 마지막 순번 */
    synchronized long append(List<String> jsonBodies) throws InterruptedException {
        while (bytes() > maxBytes) {
            if (!blocked) {
                log.warn("변경 로그가 상한({} bytes)을 넘었다 — 소비자 ack 를 기다리며 슬롯 읽기를 멈춘다 (ack={})", maxBytes, acked);
            }
            blocked = true;
            wait(1000);
        }
        blocked = false;
        if (jsonBodies.isEmpty()) {
            return lastSeq;
        }
        long t0 = System.nanoTime();
        try {
            Segment seg = segments.lastEntry().getValue();
            if (seg.committedBytes >= segmentBytes) {
                seg = roll();
            }
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(jsonBodies.size() * 512);
            List<long[]> marks = new ArrayList<>();
            long pos = seg.committedBytes;
            long seq = lastSeq;
            for (String body : jsonBodies) {
                seq++;
                if ((seq - seg.firstSeq) % INDEX_EVERY == 0) {
                    marks.add(new long[] {seq, pos + out.size()});
                }
                out.writeBytes((seq + "\t" + body + "\n").getBytes(StandardCharsets.UTF_8));
            }
            ByteBuffer buf = ByteBuffer.wrap(out.toByteArray());
            int len = buf.remaining();
            while (buf.hasRemaining()) {
                active.write(buf);
            }
            active.force(false);
            for (long[] m : marks) {
                seg.index.put(m[0], m[1]);
            }
            seg.committedBytes = pos + len;
            seg.lastSeq = seq;
            lastSeq = seq;
            notifyAll();
            return seq;
        } catch (IOException e) {
            throw new UncheckedIOException("변경 로그에 쓰지 못했다", e);
        } finally {
            appendTimer.record(System.nanoTime() - t0, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    private Segment roll() throws IOException {
        active.close();
        Segment seg = new Segment(lastSeq + 1, dir.resolve(String.format("%020d.log", lastSeq + 1)));
        active = FileChannel.open(seg.path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        segments.put(seg.firstSeq, seg);
        return seg;
    }

    // ── 읽기 ────────────────────────────────────────────────────────────────

    /** from 이 지워진 구간이면 */
    static final class GoneException extends RuntimeException {
        GoneException(String m) {
            super(m);
        }
    }

    /** from 부터 최대 max 줄. 아직 없으면 waitMs 까지 기다렸다 빈 목록 */
    List<String> read(long from, int max, long waitMs) throws InterruptedException {
        synchronized (this) {
            long deadline = System.currentTimeMillis() + waitMs;
            while (lastSeq < from) {
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) {
                    break;
                }
                wait(left);
            }
        }
        if (from < firstSeq()) {
            throw new GoneException("순번 " + from + " 은 지워졌다 (남은 가장 앞 " + firstSeq() + ")");
        }
        List<String> out = new ArrayList<>(Math.min(max, 4096));
        long want = from;
        while (out.size() < max) {
            Map.Entry<Long, Segment> e = segments.floorEntry(want);
            if (e == null || e.getValue().lastSeq < want) {
                break;
            }
            readSegment(e.getValue(), want, max - out.size(), out);
            want = from + out.size();
        }
        return out;
    }

    private void readSegment(Segment seg, long from, int max, List<String> out) {
        long limit = seg.committedBytes;
        Map.Entry<Long, Long> mark = seg.index.floorEntry(from);
        long start = mark == null ? 0 : mark.getValue();
        try (FileChannel ch = FileChannel.open(seg.path, StandardOpenOption.READ)) {
            ch.position(start);
            BufferedReader r = new BufferedReader(new InputStreamReader(
                    Channels.newInputStream(new BoundedChannel(ch, limit)), StandardCharsets.UTF_8), 1 << 16);
            String line;
            int n = 0;
            while (n < max && (line = r.readLine()) != null) {
                long seq = Long.parseLong(line, 0, line.indexOf('\t'), 10);
                if (seq >= from) {
                    out.add(line);
                    n++;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("변경 로그를 읽지 못했다: " + seg.path, e);
        }
    }

    // ── 보존 ────────────────────────────────────────────────────────────────

    /** 소비자가 seq 까지 반영했다. 등록된 소비자 전부가 지나간 세그먼트를 지운다 */
    synchronized void ack(String consumer, long seq) {
        acked.merge(consumer, Math.min(seq, lastSeq), Math::max);
        saveAcks();
        long safe = consumers.isEmpty() ? 0 : consumers.stream().mapToLong(this::ackedOf).min().orElse(0);
        for (Segment s : List.copyOf(segments.values())) {
            if (s == segments.lastEntry().getValue() || s.lastSeq > safe) {
                break;
            }
            try {
                Files.deleteIfExists(s.path);
                segments.remove(s.firstSeq);
            } catch (IOException e) {
                log.warn("세그먼트를 지우지 못했다 (다음 ack 에 다시): {}", s.path, e);
                break;
            }
        }
        notifyAll();   // 상한에 막힌 덧붙이기를 깨운다
    }

    long ackedOf(String consumer) {
        return acked.getOrDefault(consumer, 0L);
    }

    Map<String, Long> acked() {
        return new TreeMap<>(acked);
    }

    long firstSeq() {
        Map.Entry<Long, Segment> e = segments.firstEntry();
        return e == null ? 1 : e.getKey();
    }

    synchronized long lastSeq() {
        return lastSeq;
    }

    long bytes() {
        return segments.values().stream().mapToLong(s -> s.committedBytes).sum();
    }

    // ── 기동 ────────────────────────────────────────────────────────────────

    private void recover() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : files.filter(f -> f.getFileName().toString().endsWith(".log")).sorted().toList()) {
                long first = Long.parseLong(p.getFileName().toString().replace(".log", ""));
                segments.put(first, scan(new Segment(first, p)));
            }
        }
        if (segments.isEmpty()) {
            Segment seg = new Segment(1, dir.resolve(String.format("%020d.log", 1)));
            Files.createFile(seg.path);
            segments.put(1L, seg);
        }
        Segment tail = segments.lastEntry().getValue();
        lastSeq = tail.lastSeq;
        active = FileChannel.open(tail.path, StandardOpenOption.WRITE);
        active.truncate(tail.committedBytes);   // 반쯤 쓴 줄
        active.position(tail.committedBytes);
        loadAcks();
        log.info("변경 로그 열림: {} · 세그먼트 {} · 순번 {}~{} · {} bytes · ack {}", dir, segments.size(), firstSeq(),
                lastSeq, bytes(), acked);
    }

    /** 세그먼트를 처음부터 읽어 색인 · 마지막 순번 · 온전한 길이를 되살린다 */
    private static Segment scan(Segment seg) throws IOException {
        byte[] all = Files.readAllBytes(seg.path);
        int lineStart = 0;
        for (int i = 0; i < all.length; i++) {
            if (all[i] != '\n') {
                continue;
            }
            int tab = lineStart;
            while (tab < i && all[tab] != '\t') {
                tab++;
            }
            long seq = Long.parseLong(new String(all, lineStart, tab - lineStart, StandardCharsets.US_ASCII));
            if ((seq - seg.firstSeq) % INDEX_EVERY == 0) {
                seg.index.put(seq, (long) lineStart);
            }
            seg.lastSeq = seq;
            lineStart = i + 1;
            seg.committedBytes = lineStart;
        }
        return seg;
    }

    private void loadAcks() throws IOException {
        if (Files.exists(ackFile)) {
            Properties p = new Properties();
            try (var in = Files.newInputStream(ackFile)) {
                p.load(in);
            }
            p.forEach((k, v) -> acked.put((String) k, Long.parseLong((String) v)));
        }
    }

    private void saveAcks() {
        Properties p = new Properties();
        acked.forEach((k, v) -> p.setProperty(k, Long.toString(v)));
        Path tmp = dir.resolve("acks.properties.tmp");
        try {
            try (var out = Files.newOutputStream(tmp)) {
                p.store(out, null);
            }
            Files.move(tmp, ackFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn("ack 를 남기지 못했다 (보존이 조금 길어질 뿐이다): {}", e.toString());
        }
    }

    @Override
    public synchronized void close() throws IOException {
        active.close();
    }

    /** 읽기를 fsync 된 길이까지로 자른다 — 쓰는 중인 꼬리를 보지 않게 */
    private record BoundedChannel(FileChannel ch, long limit) implements java.nio.channels.ReadableByteChannel {
        @Override
        public int read(ByteBuffer dst) throws IOException {
            long left = limit - ch.position();
            if (left <= 0) {
                return -1;
            }
            if (dst.remaining() > left) {
                ByteBuffer slice = dst.slice(dst.position(), (int) left);
                int n = ch.read(slice);
                if (n > 0) {
                    dst.position(dst.position() + n);
                }
                return n;
            }
            return ch.read(dst);
        }

        @Override
        public boolean isOpen() {
            return ch.isOpen();
        }

        @Override
        public void close() throws IOException {
            ch.close();
        }
    }
}
