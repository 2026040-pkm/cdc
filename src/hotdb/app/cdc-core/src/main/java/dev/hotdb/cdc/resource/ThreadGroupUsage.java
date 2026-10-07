package dev.hotdb.cdc.resource;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 스레드 그룹 하나가 쓴 CPU · 힙 할당 누적. 새 스레드는 만든 스레드의 그룹을 물려받으므로, 일을 이 그룹 안에서
 * 시작하면 그 일이 띄우는 스레드까지 이름과 상관없이 다 잡힌다.
 */
public final class ThreadGroupUsage {

    private static final com.sun.management.ThreadMXBean THREADS =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    static {
        THREADS.setThreadAllocatedMemoryEnabled(true);
    }

    private final ThreadGroup group;
    /** 스레드마다 마지막으로 본 누적값. 끝난 스레드 몫도 남겨 두어야 합이 줄지 않는다 */
    private final Map<Long, long[]> seen = new ConcurrentHashMap<>();

    public ThreadGroupUsage(String name) {
        this.group = new ThreadGroup(name);
    }

    public ThreadGroup group() {
        return group;
    }

    public int liveThreads() {
        return group.activeCount();
    }

    /** [CPU ns, 할당 bytes] — 그룹 스레드 누적. 읽을 때마다 살아 있는 스레드 값을 갱신한다 */
    public synchronized long[] totals() {
        Thread[] live = new Thread[group.activeCount() + 8];
        int n = group.enumerate(live, true);
        for (int i = 0; i < n; i++) {
            long id = live[i].threadId();
            long cpu = THREADS.getThreadCpuTime(id);
            long alloc = THREADS.getThreadAllocatedBytes(id);
            if (cpu >= 0 && alloc >= 0) {
                seen.put(id, new long[] {cpu, alloc});
            }
        }
        long cpu = 0;
        long alloc = 0;
        for (long[] v : seen.values()) {
            cpu += v[0];
            alloc += v[1];
        }
        return new long[] {cpu, alloc};
    }
}
