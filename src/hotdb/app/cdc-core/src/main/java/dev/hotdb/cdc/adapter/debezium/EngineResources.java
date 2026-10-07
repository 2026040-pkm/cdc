package dev.hotdb.cdc.adapter.debezium;

import dev.hotdb.cdc.resource.ThreadGroupUsage;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Embedded 엔진이 프로세스 안에서 쓰는 CPU · 할당 · 스레드를 프로세스 전체와 따로 센다.
 *
 * <p>엔진 스레드는 이름이 제각각이라(pool-N-thread-M · debezium-* · FileOffsetBackingStore-N · Thread-N)
 * 이름으로는 못 고른다. 대신 엔진을 이 스레드 그룹 안에서 만들고 돌린다 — 새 스레드는 만든 스레드의 그룹을 물려받으므로
 * Debezium 이 안에서 띄우는 스레드가 전부 여기 들어온다.
 *
 * <p>part 태그:
 * <ul>
 *   <li>{@code engine} — 그룹 스레드 전체에서 handler 몫을 뺀 것. Debezium 이 슬롯을 읽고 해석하고 큐에 넣고 꺼내는 일
 *   <li>{@code handler} — 배치 콜백 안(우리 해석 + 싱크 반영 + 체크포인트). 엔진 스레드 위에서 돌지만 엔진 몫이 아니다
 *   <li>{@code process} — 프로세스 전체 (GC · JIT 같은 JVM 내부 스레드 포함). 나머지 = process - engine - handler
 * </ul>
 * GC 비용은 할당한 쪽에 붙지 않고 JVM 스레드로 잡힌다 — 할당량(alloc)으로 누가 GC 를 부르는지 본다.
 */
final class EngineResources {

    private final com.sun.management.ThreadMXBean threads =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    private final ThreadGroupUsage usage;
    private final com.sun.management.OperatingSystemMXBean os =
            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final MBeanServer mbeans = ManagementFactory.getPlatformMBeanServer();
    private final ObjectName streamingMetrics;
    private final AtomicLong handlerCpuNs = new AtomicLong();
    private final AtomicLong handlerAllocBytes = new AtomicLong();

    EngineResources(String pipeline, MeterRegistry meters) {
        this.usage = new ThreadGroupUsage("cdc-engine-" + pipeline);
        this.streamingMetrics = objectName(
                "debezium.postgres:type=connector-metrics,context=streaming,server=" + pipeline);

        FunctionCounter.builder("hotdb.cdc.engine.cpu", this, r -> r.usage.totals()[0] / 1e9 - r.handlerCpuNs.get() / 1e9)
                .baseUnit("seconds").tag("part", "engine")
                .description("Embedded 엔진 스레드 CPU (배치 콜백 몫 제외)").register(meters);
        FunctionCounter.builder("hotdb.cdc.engine.cpu", this, r -> r.handlerCpuNs.get() / 1e9)
                .baseUnit("seconds").tag("part", "handler").register(meters);
        FunctionCounter.builder("hotdb.cdc.engine.cpu", this, r -> r.os.getProcessCpuTime() / 1e9)
                .baseUnit("seconds").tag("part", "process").register(meters);

        FunctionCounter.builder("hotdb.cdc.engine.alloc", this, r -> r.usage.totals()[1] - r.handlerAllocBytes.get())
                .baseUnit("bytes").tag("part", "engine")
                .description("힙 할당량 누적 — GC 를 부르는 쪽").register(meters);
        FunctionCounter.builder("hotdb.cdc.engine.alloc", this, r -> r.handlerAllocBytes.get())
                .baseUnit("bytes").tag("part", "handler").register(meters);
        FunctionCounter.builder("hotdb.cdc.engine.alloc", this, r -> r.threads.getTotalThreadAllocatedBytes())
                .baseUnit("bytes").tag("part", "process").register(meters);

        Gauge.builder("hotdb.cdc.engine.threads", this, r -> r.usage.liveThreads())
                .description("엔진 스레드 그룹의 살아 있는 스레드 수").register(meters);
        // Debezium 내부 큐 (max.queue.size). 꽉 차면 슬롯 읽기가 멈춘다 — 적체 때 메모리 상한이 여기서 정해진다
        Gauge.builder("hotdb.cdc.engine.queue", this, r -> r.streaming("QueueTotalCapacity") - r.streaming("QueueRemainingCapacity"))
                .tag("state", "used").register(meters);
        Gauge.builder("hotdb.cdc.engine.queue", this, r -> r.streaming("QueueTotalCapacity"))
                .tag("state", "capacity").register(meters);
        // 바이트 상한(max.queue.size.in.bytes). 0 = 건수(max.queue.size)로만 막는다
        Gauge.builder("hotdb.cdc.engine.queue.bytes", this, r -> r.streaming("CurrentQueueSizeInBytes"))
                .baseUnit("bytes").tag("state", "used").register(meters);
        Gauge.builder("hotdb.cdc.engine.queue.bytes", this, r -> r.streaming("MaxQueueSizeInBytes"))
                .baseUnit("bytes").tag("state", "capacity").register(meters);
        Gauge.builder("hotdb.cdc.engine.behind.source", this, r -> r.streaming("MilliSecondsBehindSource") / 1000.0)
                .baseUnit("seconds").description("Debezium 이 본 원천 대비 지연").register(meters);
    }

    ThreadGroup group() {
        return usage.group();
    }

    /** 배치 콜백 시작 시점의 현재 스레드 CPU · 할당. {@link #endHandler} 에 넘긴다 */
    long[] startHandler() {
        return new long[] {threads.getCurrentThreadCpuTime(), threads.getCurrentThreadAllocatedBytes()};
    }

    void endHandler(long[] start) {
        handlerCpuNs.addAndGet(threads.getCurrentThreadCpuTime() - start[0]);
        handlerAllocBytes.addAndGet(threads.getCurrentThreadAllocatedBytes() - start[1]);
    }

    private double streaming(String attribute) {
        try {
            return ((Number) mbeans.getAttribute(streamingMetrics, attribute)).doubleValue();
        } catch (Exception e) {
            return Double.NaN;   // 엔진이 아직 안 떴거나 이미 내려갔다
        }
    }

    private static ObjectName objectName(String name) {
        try {
            return new ObjectName(name);
        } catch (Exception e) {
            throw new IllegalArgumentException(name, e);
        }
    }
}
