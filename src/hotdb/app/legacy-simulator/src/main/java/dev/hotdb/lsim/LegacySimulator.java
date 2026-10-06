package dev.hotdb.lsim;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 레거시 대역의 원천 표를 주기마다 바꾼다 — 표마다 몇 행 UPDATE(워터마크 = 지금) + 몇 행 INSERT.
 *
 * <p>표 모양은 기동 때 JDBC 메타데이터로 읽는다. 시험용 표의 컬럼은 글자(text · VARCHAR2) · 숫자(numeric · NUMBER) ·
 * 날짜(date · DATE) 셋뿐이라 그 셋만 만든다. 워터마크 컬럼({@code upd_date} = yyyyMMdd · {@code upd_time} = HHmmss)이
 * 있는 표는 그 값을 지금으로 — RFC Service · DB Agent 의 증분 폴링이 다음 주기에 가져간다. 없는 표(replace 작업)는
 * 행이 늘어나는 것으로 확인한다.
 */
@Component
public class LegacySimulator {

    private static final Logger log = LoggerFactory.getLogger(LegacySimulator.class);
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter YMD = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter HMS = DateTimeFormatter.ofPattern("HHmmss");

    /** 원천 하나 — SAP 대역(PostgreSQL) 또는 Oracle 대역. */
    record Source(String name, Dialect dialect, JdbcTemplate jdbc, List<Table> tables) {}

    record Table(String display, String sql, List<Col> cols, Set<String> pk, Col updDate, Col updTime, Col changeCol) {
        boolean watermarked() {
            return updDate != null && updTime != null;
        }
    }

    record Col(String name, String quoted, Kind kind) {}

    enum Kind { TEXT, NUMBER, DATE }

    enum Dialect {
        POSTGRES {
            @Override String quote(String id) { return "\"" + id + "\""; }
            @Override String randomRows(String table, int n) {
                return "ctid IN (SELECT ctid FROM " + table + " ORDER BY random() LIMIT " + n + ")";
            }
        },
        ORACLE {
            @Override String quote(String id) { return "\"" + id + "\""; }
            @Override String randomRows(String table, int n) {
                return "ROWID IN (SELECT rid FROM (SELECT ROWID rid FROM " + table + " ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM <= " + n + ")";
            }
        };

        abstract String quote(String id);

        abstract String randomRows(String table, int n);
    }

    private final LsimProperties props;
    private final MeterRegistry meters;
    private final List<Source> sources = new ArrayList<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "lsim");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, AtomicLong> rows = new LinkedHashMap<>();
    private final AtomicLong seq = new AtomicLong((System.currentTimeMillis() / 1000) % 100_000_000L * 100);
    private final Timer tickTimer;
    private volatile double speed;
    private volatile boolean running;
    private volatile long ticks;
    private volatile String lastError;
    private ScheduledFuture<?> task;

    public LegacySimulator(LsimProperties props, MeterRegistry meters) {
        this.props = props;
        this.meters = meters;
        this.speed = props.speed();
        for (String s : List.of("sap", "oracle")) {
            for (String op : List.of("updated", "inserted")) {
                AtomicLong c = new AtomicLong();
                rows.put(s + "." + op, c);
                meters.more().counter("hotdb.lsim.rows", Tags.of("source", s, "op", op), c);
            }
            meters.counter("hotdb.lsim.errors", "source", s);
        }
        meters.gauge("hotdb.lsim.speed", this, x -> x.speed);
        meters.gauge("hotdb.lsim.running", this, x -> x.running ? 1 : 0);
        meters.gauge("hotdb.lsim.tables", this, x -> x.sources.stream().mapToInt(src -> src.tables().size()).sum());
        tickTimer = Timer.builder("hotdb.lsim.tick").publishPercentileHistogram().register(meters);
    }

    // ── 기동: 표 모양 읽기 ────────────────────────────────────────────────────

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        LsimProperties.Sap sap = props.sap();
        List<String> sapTables = sap.tables() == null ? List.of()
                : sap.tables().stream().map(String::trim).filter(s -> !s.isEmpty()).toList();
        sources.add(new Source("sap", Dialect.POSTGRES, jdbc(sap.url(), sap.user(), sap.password()), new ArrayList<>()));
        sources.add(new Source("oracle", Dialect.ORACLE, jdbc(props.oracle().url(), props.oracle().user(), props.oracle().password()), new ArrayList<>()));
        discover(sources.get(0), List.of(sap.schema()), sapTables);
        discover(sources.get(1), props.oracle().schemas().stream().map(s -> s.trim().toUpperCase(Locale.ROOT)).toList(), List.of());
        for (Source s : sources) {
            long wm = s.tables().stream().filter(Table::watermarked).count();
            log.info("{} 대역: 표 {}개 (워터마크 있는 표 {})", s.name(), s.tables().size(), wm);
        }
        if (props.autoStart()) {
            resume();
        }
    }

    private static JdbcTemplate jdbc(String url, String user, String password) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(user);
        ds.setPassword(password);
        ds.setMaximumPoolSize(2);
        ds.setInitializationFailTimeout(-1);     // 대역이 아직 안 떠 있어도 기동은 하고, 주기마다 다시 시도한다
        ds.setConnectionTimeout(10_000);
        return new JdbcTemplate(ds);
    }

    /** 스키마들의 표 · 컬럼 · PK 를 읽는다. 표 목록을 주면 그것만, 아니면 전부 (SAP 쪽은 Z 표 = 송신 대상이라 뺀다, Oracle 은 휴지통 BIN$ 만). */
    private void discover(Source src, List<String> schemas, List<String> only) {
        boolean skipZ = src.dialect() == Dialect.POSTGRES;
        try (Connection c = src.jdbc().getDataSource().getConnection()) {
            DatabaseMetaData md = c.getMetaData();
            for (String schema : schemas) {
                List<String> names = new ArrayList<>();
                try (ResultSet rs = md.getTables(null, schema, "%", new String[]{"TABLE"})) {
                    while (rs.next()) {
                        String n = rs.getString("TABLE_NAME");
                        String lower = n.toLowerCase(Locale.ROOT);
                        if (!only.isEmpty()) {
                            if (only.stream().anyMatch(o -> o.equalsIgnoreCase(n))) names.add(n);
                        } else if (!(skipZ && lower.startsWith("z")) && !n.startsWith("BIN$")) {
                            names.add(n);
                        }
                    }
                }
                for (String n : names) {
                    src.tables().add(describe(src.dialect(), md, schema, n));
                }
            }
        } catch (SQLException e) {
            lastError = src.name() + " 표 읽기 실패: " + e.getMessage();
            log.warn("{} 대역 표를 못 읽었습니다 — 비워 두고 돈다. 대역을 띄운 뒤 재기동하세요: {}", src.name(), e.getMessage());
        }
    }

    private static Table describe(Dialect d, DatabaseMetaData md, String schema, String table) throws SQLException {
        List<Col> cols = new ArrayList<>();
        try (ResultSet rs = md.getColumns(null, schema, table, "%")) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                cols.add(new Col(name, d.quote(name), kind(rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME"))));
            }
        }
        Set<String> pk = new LinkedHashSet<>();
        try (ResultSet rs = md.getPrimaryKeys(null, schema, table)) {
            while (rs.next()) {
                pk.add(rs.getString("COLUMN_NAME"));
            }
        }
        Col updDate = null, updTime = null, change = null;
        for (Col c : cols) {
            String lower = c.name().toLowerCase(Locale.ROOT);
            if (lower.equals("upd_date")) updDate = c;
            else if (lower.equals("upd_time")) updTime = c;
            else if (change == null && c.kind() == Kind.TEXT && !pk.contains(c.name())) change = c;
        }
        return new Table(schema + "." + table, d.quote(schema) + "." + d.quote(table), cols, pk, updDate, updTime, change);
    }

    private static Kind kind(int jdbcType, String typeName) {
        switch (jdbcType) {
            case Types.NUMERIC, Types.DECIMAL, Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.TINYINT,
                 Types.DOUBLE, Types.FLOAT, Types.REAL -> { return Kind.NUMBER; }
            case Types.DATE, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> { return Kind.DATE; }
            default -> {
                String t = typeName == null ? "" : typeName.toUpperCase(Locale.ROOT);
                return t.contains("DATE") || t.contains("TIME") ? Kind.DATE : t.equals("NUMBER") ? Kind.NUMBER : Kind.TEXT;
            }
        }
    }

    // ── 제어 ───────────────────────────────────────────────────────────────

    public synchronized void resume() {
        if (running) {
            return;
        }
        running = true;
        long period = Math.max(200, (long) (props.intervalMs() / speed));
        task = scheduler.scheduleAtFixedRate(this::guardedTick, 0, period, TimeUnit.MILLISECONDS);
        log.info("레거시 발행 시작: 배속 x{} (주기 {}ms, 표마다 UPDATE {} · INSERT {})", speed, period,
                props.updatesPerTable(), props.insertsPerTable());
    }

    public synchronized void pause() {
        if (!running) {
            return;
        }
        running = false;
        task.cancel(false);
        log.info("레거시 발행 멈춤");
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
        out.put("intervalMs", (long) (props.intervalMs() / speed));
        out.put("updatesPerTable", props.updatesPerTable());
        out.put("insertsPerTable", props.insertsPerTable());
        Map<String, Object> tables = new LinkedHashMap<>();
        for (Source s : sources) {
            tables.put(s.name(), Map.of("tables", s.tables().size(),
                    "watermarked", s.tables().stream().filter(Table::watermarked).count()));
        }
        out.put("sources", tables);
        out.put("ticks", ticks);
        Map<String, Long> totals = new LinkedHashMap<>();
        rows.forEach((k, v) -> totals.put(k, v.get()));
        out.put("rowsTotal", totals);
        if (lastError != null) {
            out.put("lastError", lastError);
        }
        return out;
    }

    private void guardedTick() {
        try {
            tickTimer.record(this::tick);
        } catch (RuntimeException e) {
            // 예외가 나가면 scheduleAtFixedRate 가 조용히 멈춘다 — 기록만 하고 다음 주기에 다시 한다
            lastError = e.getMessage();
            log.warn("주기 실패: {}", e.getMessage());
        }
    }

    // ── 한 주기: 원천마다 · 표마다 UPDATE + INSERT ─────────────────────────────

    public void tick() {
        LocalDateTime now = LocalDateTime.now(KST);
        for (Source s : sources) {
            long upd = 0, ins = 0;
            for (Table t : s.tables()) {
                try {
                    upd += update(s, t, now);
                    ins += insert(s, t, now);
                } catch (RuntimeException e) {
                    meters.counter("hotdb.lsim.errors", "source", s.name()).increment();
                    lastError = t.display() + ": " + rootMessage(e);
                    if (ticks % 30 == 0) {           // 대역이 내려가 있으면 표마다 찍히므로 가끔만
                        log.warn("{} 실패: {}", t.display(), rootMessage(e));
                    }
                }
            }
            rows.get(s.name() + ".updated").addAndGet(upd);
            rows.get(s.name() + ".inserted").addAndGet(ins);
        }
        ticks++;
        if (ticks == 1 || ticks % 30 == 0) {
            log.info("주기 {} 누적: {}", ticks, rows.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue().get()).collect(Collectors.joining(" · ")));
        }
    }

    private int update(Source s, Table t, LocalDateTime now) {
        int n = props.updatesPerTable();
        if (n <= 0) {
            return 0;
        }
        List<String> sets = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        if (t.watermarked()) {
            sets.add(t.updDate().quoted() + " = ?");
            args.add(now.format(YMD));
            sets.add(t.updTime().quoted() + " = ?");
            args.add(now.format(HMS));
        }
        if (t.changeCol() != null) {
            sets.add(t.changeCol().quoted() + " = ?");
            args.add("CHG-" + now.format(HMS));
        }
        if (sets.isEmpty()) {
            return 0;
        }
        String sql = "UPDATE " + t.sql() + " SET " + String.join(", ", sets) + " WHERE " + s.dialect().randomRows(t.sql(), n);
        return s.jdbc().update(sql, args.toArray());
    }

    private int insert(Source s, Table t, LocalDateTime now) {
        int n = props.insertsPerTable();
        if (n <= 0 || t.cols().isEmpty()) {
            return 0;
        }
        String names = t.cols().stream().map(Col::quoted).collect(Collectors.joining(", "));
        String marks = t.cols().stream().map(c -> "?").collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + t.sql() + " (" + names + ") VALUES (" + marks + ")";
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        s.jdbc().batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                long id = seq.incrementAndGet();
                int p = 1;
                for (Col c : t.cols()) {
                    ps.setObject(p++, value(t, c, id, now, rnd));
                }
            }

            @Override
            public int getBatchSize() {
                return n;
            }
        });
        return n;
    }

    /** scripts/legacy-sim.py 의 value() 와 같은 규칙. PK 는 LSIM + 일련번호라 시드(KEY_1 00001 …)와 안 겹친다. */
    private static Object value(Table t, Col c, long id, LocalDateTime now, ThreadLocalRandom rnd) {
        String lower = c.name().toLowerCase(Locale.ROOT);
        if (lower.equals("upd_date")) {
            return now.format(YMD);
        }
        if (lower.equals("upd_time")) {
            return now.format(HMS);
        }
        boolean keyed = t.pk().contains(c.name());
        return switch (c.kind()) {
            case NUMBER -> keyed ? BigDecimal.valueOf(id)
                    : BigDecimal.valueOf(rnd.nextDouble() * 1000).setScale(2, RoundingMode.HALF_UP);
            case DATE -> Date.valueOf(LocalDate.now(KST).minusDays(rnd.nextInt(30)));
            case TEXT -> keyed ? "LSIM" + String.format("%010d", id)
                    : "SIM-" + lower.substring(0, Math.min(8, lower.length())) + "-" + rnd.nextInt(1000);
        };
    }

    private static String rootMessage(Throwable e) {
        Throwable r = e;
        while (r.getCause() != null && r.getCause() != r) {
            r = r.getCause();
        }
        String m = r.getMessage() == null ? r.getClass().getSimpleName() : r.getMessage();
        return m.length() > 200 ? m.substring(0, 200) : m;
    }
}
