package dev.hotdb.capture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileChangeLogTest {

    @TempDir
    Path dir;

    FileChangeLog open(long segment, long max) throws Exception {
        return new FileChangeLog(dir, segment, max, List.of("apply-asm"), new SimpleMeterRegistry());
    }

    static List<String> bodies(int n, String prefix) {
        return IntStream.range(0, n).mapToObj(i -> "{\"v\":\"" + prefix + i + "\"}").toList();
    }

    @Test
    void 순번은_빈틈없이_하나씩_늘고_세그먼트를_넘어도_이어서_읽힌다() throws Exception {
        try (FileChangeLog log = open(2_000, Long.MAX_VALUE)) {
            for (int i = 0; i < 10; i++) {
                log.append(bodies(100, "b" + i + "-"));
            }
            List<String> lines = log.read(250, 600, 0);

            assertThat(lines).hasSize(600);
            assertThat(lines.get(0)).startsWith("250\t");
            assertThat(lines.get(599)).startsWith("849\t");
            assertThat(Files.list(dir).filter(f -> f.toString().endsWith(".log")).count()).isEqualTo(5);   // 배치 경계에서만 넘긴다 (150B × 100줄 배치 2개 = 3KB > 2KB)
        }
    }

    @Test
    void 죽은_순간_반쯤_쓴_줄은_기동때_잘라낸다_그_줄은_슬롯도_넘어가지_않았다() throws Exception {
        try (FileChangeLog log = open(1 << 20, Long.MAX_VALUE)) {
            log.append(bodies(5, "x"));
        }
        Path seg = dir.resolve(String.format("%020d.log", 1));
        Files.writeString(seg, "6\t{\"v\":\"반쯤", StandardCharsets.UTF_8, StandardOpenOption.APPEND);

        try (FileChangeLog log = open(1 << 20, Long.MAX_VALUE)) {
            assertThat(log.lastSeq()).isEqualTo(5);
            assertThat(log.append(bodies(1, "y"))).isEqualTo(6);
            assertThat(log.read(6, 10, 0)).containsExactly("6\t{\"v\":\"y0\"}");
        }
    }

    @Test
    void 등록된_소비자가_ack_한_세그먼트만_지우고_지운_구간을_달라하면_Gone() throws Exception {
        try (FileChangeLog log = open(2_000, Long.MAX_VALUE)) {
            for (int i = 0; i < 10; i++) {
                log.append(bodies(100, "b"));
            }
            log.ack("someone-else", 1000);   // 등록 안 된 소비자는 보존 기준이 아니다
            assertThat(log.firstSeq()).isEqualTo(1);

            log.ack("apply-asm", 700);
            assertThat(log.firstSeq()).isGreaterThan(1).isLessThanOrEqualTo(701);
            assertThatThrownBy(() -> log.read(1, 10, 0)).isInstanceOf(FileChangeLog.GoneException.class);
            assertThat(log.read(701, 10, 0).get(0)).startsWith("701\t");
        }
    }

    @Test
    void 상한을_넘으면_덧붙이기가_ack_를_기다린다_그동안_슬롯이_WAL_을_쥔다() throws Exception {
        try (FileChangeLog log = open(1_000, 3_000)) {
            while (log.bytes() <= 3_000) {
                log.append(bodies(20, "f"));
            }
            long last = log.lastSeq();
            CompletableFuture<Long> blocked = CompletableFuture.supplyAsync(() -> {
                try {
                    return log.append(bodies(1, "late"));
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
            Thread.sleep(300);
            assertThat(blocked).isNotDone();

            log.ack("apply-asm", last);
            assertThat(blocked.get(5, TimeUnit.SECONDS)).isEqualTo(last + 1);
        }
    }

    @Test
    void 새_줄이_없으면_기다렸다가_들어오면_바로_돌려준다() throws Exception {
        try (FileChangeLog log = open(1 << 20, Long.MAX_VALUE)) {
            log.append(bodies(3, "a"));
            CompletableFuture<List<String>> waiting = CompletableFuture.supplyAsync(() -> {
                try {
                    return log.read(4, 10, 5_000);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
            Thread.sleep(200);
            log.append(bodies(2, "b"));
            assertThat(waiting.get(2, TimeUnit.SECONDS)).hasSize(2);
        }
    }
}
