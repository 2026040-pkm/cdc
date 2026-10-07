package dev.hotdb.cdc.resource;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 서비스 본연의 일(판정 · 폴링 …)을 하는 스레드를 part 이름으로 묶어 센다 — CDC 엔진(hotdb.cdc.engine.*)과
 * 같은 방식이라 한 프로세스 안에서 엔진과 본연의 일이 CPU · 할당을 어떻게 나눠 쓰는지 나란히 볼 수 있다.
 *
 * <pre>
 * hotdb_work_cpu_seconds_total{part}    hotdb_work_alloc_bytes_total{part}    hotdb_work_threads{part}
 * </pre>
 * 같은 part 를 여러 번 부르면 같은 그룹에 모인다.
 */
public final class WorkThreads {

    private static final Map<String, ThreadGroupUsage> PARTS = new ConcurrentHashMap<>();

    private WorkThreads() {}

    /** part 그룹 안에 스레드를 만드는 팩토리. 스레드 이름은 {@code <threadName>} 또는 {@code <threadName>-N} */
    public static ThreadFactory factory(String part, String threadName, MeterRegistry meters) {
        // 레지스트리마다 따로 — 테스트처럼 컨텍스트가 여럿이어도 각자 지표를 받는다
        ThreadGroupUsage usage = PARTS.computeIfAbsent(part + "@" + System.identityHashCode(meters),
                k -> register(part, meters));
        AtomicInteger seq = new AtomicInteger();
        return r -> {
            int n = seq.getAndIncrement();
            return new Thread(usage.group(), r, n == 0 ? threadName : threadName + "-" + n);
        };
    }

    private static ThreadGroupUsage register(String part, MeterRegistry meters) {
        ThreadGroupUsage u = new ThreadGroupUsage("work-" + part);
        FunctionCounter.builder("hotdb.work.cpu", u, g -> g.totals()[0] / 1e9).baseUnit("seconds").tag("part", part)
                .description("서비스 본연의 일(판정 · 폴링 …) 스레드 CPU").register(meters);
        FunctionCounter.builder("hotdb.work.alloc", u, g -> g.totals()[1]).baseUnit("bytes").tag("part", part)
                .description("서비스 본연의 일 스레드 힙 할당 누적").register(meters);
        Gauge.builder("hotdb.work.threads", u, ThreadGroupUsage::liveThreads).tag("part", part).register(meters);
        return u;
    }
}
