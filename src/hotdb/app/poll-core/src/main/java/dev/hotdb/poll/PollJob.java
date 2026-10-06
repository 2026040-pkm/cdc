package dev.hotdb.poll;

import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.route.CompiledRoute.Column;
import dev.hotdb.cdc.route.Expr;
import dev.hotdb.cdc.route.ExprParser.Policy;
import dev.hotdb.cdc.route.RouteMode;
import dev.hotdb.cdc.route.SqlBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 작업 하나: 원천 표를 읽어 HotDB 레거시 표에 쓴다.
 *
 * <pre>
 *   incremental : SELECT 컬럼, (워터마크) FROM 원천 WHERE (워터마크) &gt;= 지난값   → UPSERT → 워터마크 저장
 *   full        : SELECT 컬럼 FROM 원천                                           → UPSERT
 *   replace     : 대상 DELETE + SELECT 전부 INSERT (한 트랜잭션) — PK 없는 표
 * </pre>
 *
 * <p>워터마크는 모든 행을 쓴 뒤에만 저장한다. 중간에 실패하면 다음 주기에 같은 자리부터 다시 읽고,
 * UPSERT 라 결과가 같다. 같은 워터마크 값의 행은 매번 다시 읽힌다(>=) — 같은 초에 바뀐 행을 놓치지 않기 위해서다.
 */
public class PollJob {

    private static final Logger log = LoggerFactory.getLogger(PollJob.class);
    private static final String WM_ALIAS = "HOTDB_WM";

    private final String agent;
    private final PollProperties.JobSpec spec;
    private final DataSource source;
    private final JdbcTemplate target;
    private final TransactionTemplate tx;
    private final Map<String, String> targetTypes;   // 대상 컬럼(소문자) → 타입
    private final List<String> keys;
    private final int fetchSize;
    private final int batchSize;
    private final MeterRegistry meters;
    private final Timer runTimer;
    private volatile Instant lastOkAt;

    public PollJob(String agent, PollProperties.JobSpec spec, DataSource source, JdbcTemplate target,
                   TransactionTemplate tx, Map<String, String> targetTypes, List<String> targetPk,
                   int fetchSize, int batchSize, MeterRegistry meters) {
        this.agent = agent;
        this.spec = spec;
        this.source = source;
        this.target = target;
        this.tx = tx;
        this.targetTypes = targetTypes;
        this.keys = spec.keys().isEmpty() ? targetPk : spec.keys().stream().map(k -> k.toLowerCase(Locale.ROOT)).toList();
        this.fetchSize = fetchSize;
        this.batchSize = batchSize;
        this.meters = meters;
        this.runTimer = Timer.builder("hotdb.poll.run").tag("job", spec.jobName()).publishPercentileHistogram().register(meters);
        meters.counter("hotdb.poll.rows", "job", spec.jobName());
        meters.counter("hotdb.poll.errors", "job", spec.jobName());
        meters.gauge("hotdb.poll.last.ok.age.seconds", io.micrometer.core.instrument.Tags.of("job", spec.jobName()), this,
                j -> j.lastOkAt == null ? -1 : (Instant.now().toEpochMilli() - j.lastOkAt.toEpochMilli()) / 1000.0);

        String mode = mode();
        if (!List.of("incremental", "full", "replace").contains(mode)) {
            throw new IllegalStateException("작업 " + spec.jobName() + ": mode 는 incremental · full · replace — " + spec.mode());
        }
        if (!mode.equals("replace") && keys.isEmpty()) {
            throw new IllegalStateException("작업 " + spec.jobName() + ": 대상 " + spec.target()
                    + " 에 PK 가 없어 UPSERT 할 수 없습니다 — keys 를 주거나 mode: replace");
        }
        if (mode.equals("incremental") && (spec.watermark() == null || spec.watermark().isBlank())) {
            throw new IllegalStateException("작업 " + spec.jobName() + ": incremental 은 watermark 식이 필요합니다");
        }
        for (String k : keys) {
            if (!targetTypes.containsKey(k)) {
                throw new IllegalStateException("작업 " + spec.jobName() + ": 키 " + k + " 가 대상 " + spec.target() + " 에 없습니다");
            }
        }
    }

    public String name() {
        return spec.jobName();
    }

    public long intervalMs(long dflt) {
        return spec.intervalMs() == null ? dflt : spec.intervalMs();
    }

    private String mode() {
        return spec.mode().trim().toLowerCase(Locale.ROOT);
    }

    /** 한 주기. 예외는 안에서 기록하고 삼킨다 — 원천이 잠시 내려가도 다음 주기에 다시 한다. */
    public void runOnce() {
        Timer.Sample sample = Timer.start(meters);
        target.update("""
                INSERT INTO ops.poll_state (agent, job, last_run_at) VALUES (?, ?, clock_timestamp())
                ON CONFLICT (agent, job) DO UPDATE SET last_run_at = EXCLUDED.last_run_at
                """, agent, name());
        try {
            long rows = poll();
            lastOkAt = Instant.now();
            meters.counter("hotdb.poll.rows", "job", name()).increment(rows);
            target.update("UPDATE ops.poll_state SET last_ok_at = clock_timestamp(), last_rows = ?, last_error = NULL "
                    + "WHERE agent = ? AND job = ?", rows, agent, name());
            if (rows > 0) {
                log.info("[{}] {} → {} : {}행", name(), spec.from(), spec.target(), rows);
            }
        } catch (Exception e) {
            meters.counter("hotdb.poll.errors", "job", name()).increment();
            String msg = rootMessage(e);
            log.warn("[{}] 폴링 실패: {}", name(), msg);
            target.update("UPDATE ops.poll_state SET last_error = ? WHERE agent = ? AND job = ?", msg, agent, name());
        } finally {
            sample.stop(runTimer);
        }
    }

    long poll() throws SQLException {
        String mode = mode();
        String watermark = mode.equals("incremental")
                ? target.query("SELECT watermark FROM ops.poll_state WHERE agent = ? AND job = ?",
                        rs -> rs.next() ? rs.getString(1) : null, agent, name())
                : null;

        try (Connection c = source.getConnection()) {
            c.setReadOnly(true);
            List<String> cols = sharedColumns(c);
            String sql = selectSql(spec.from(), cols, mode.equals("incremental") ? spec.watermark() : null, watermark != null);
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setFetchSize(fetchSize);
                if (watermark != null) {
                    ps.setString(1, watermark);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    return write(rs, cols, mode, watermark);
                }
            }
        }
    }

    /** 원천 · 대상 양쪽에 있는 컬럼 (원천 표기 그대로 — Oracle · HANA 는 대문자). */
    private List<String> sharedColumns(Connection c) throws SQLException {
        List<String> out = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM " + spec.from() + " WHERE 1 = 0");
             ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String name = md.getColumnName(i);
                if (targetTypes.containsKey(name.toLowerCase(Locale.ROOT))) {
                    out.add(name);
                } else {
                    missing.add(name);
                }
            }
        }
        if (!missing.isEmpty()) {
            log.debug("[{}] 대상에 없어 건너뛰는 원천 컬럼: {}", name(), missing);
        }
        for (String k : keys) {
            if (out.stream().noneMatch(x -> x.equalsIgnoreCase(k))) {
                throw new IllegalStateException("키 " + k + " 가 원천 " + spec.from() + " 에 없습니다");
            }
        }
        return out;
    }

    static String selectSql(String from, List<String> cols, String watermarkExpr, boolean hasWatermark) {
        String list = cols.stream().map(c -> "\"" + c.replace("\"", "\"\"") + "\"").collect(Collectors.joining(", "));
        StringBuilder sb = new StringBuilder("SELECT ").append(list);
        if (watermarkExpr != null) {
            sb.append(", (").append(watermarkExpr).append(") AS ").append(WM_ALIAS);
        }
        return sb.append(" FROM ").append(from).append(hasWatermark ? " WHERE (" + watermarkExpr + ") >= ?" : "").toString();
    }

    private long write(ResultSet rs, List<String> cols, String mode, String startWatermark) throws SQLException {
        List<Column> targetCols = cols.stream()
                .map(c -> new Column(c.toLowerCase(Locale.ROOT), new Expr.Literal(null), Policy.OVERWRITE,
                        targetTypes.get(c.toLowerCase(Locale.ROOT))))
                .toList();
        TableId t = TableId.parse(spec.target());
        String sql = mode.equals("replace")
                ? SqlBuilder.build(RouteMode.INSERT, t, targetCols, List.of(), null, List.of(), List.of(), null).sql()
                        .replace(" ON CONFLICT DO NOTHING", "")
                : SqlBuilder.build(RouteMode.UPSERT, t, targetCols, keys, null, List.of(), List.of(), null).sql();

        boolean hasWm = mode.equals("incremental");
        String[] maxWm = {startWatermark};
        long[] total = {0};
        List<Object[]> batch = new ArrayList<>(batchSize);

        Runnable body = () -> {
            try {
                if (mode.equals("replace")) {
                    target.update("DELETE FROM " + q(t));
                }
                while (rs.next()) {
                    Object[] row = new Object[cols.size()];
                    for (int i = 0; i < cols.size(); i++) {
                        row[i] = PollValues.toSql(rs.getObject(i + 1));
                    }
                    if (hasWm) {
                        String wm = rs.getString(cols.size() + 1);
                        if (wm != null && (maxWm[0] == null || wm.compareTo(maxWm[0]) > 0)) {
                            maxWm[0] = wm;
                        }
                    }
                    batch.add(row);
                    if (batch.size() >= batchSize) {
                        flush(sql, batch, mode);
                        total[0] += batch.size();
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    flush(sql, batch, mode);
                    total[0] += batch.size();
                    batch.clear();
                }
                if (hasWm && maxWm[0] != null) {
                    target.update("UPDATE ops.poll_state SET watermark = ? WHERE agent = ? AND job = ?",
                            maxWm[0], agent, name());
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        };
        // replace 는 지우기와 넣기가 한 트랜잭션이어야 중간 상태(빈 표)가 안 보인다.
        // UPSERT 작업은 배치마다 커밋하고, 워터마크는 마지막에만 저장한다.
        if (mode.equals("replace")) {
            tx.executeWithoutResult(s -> body.run());
        } else {
            body.run();
        }
        return total[0];
    }

    private void flush(String sql, List<Object[]> batch, String mode) {
        if (mode.equals("replace")) {
            target.batchUpdate(sql, batch);
        } else {
            tx.executeWithoutResult(s -> target.batchUpdate(sql, batch));
        }
    }

    private static String q(TableId t) {
        return "\"" + t.schema() + "\".\"" + t.table() + "\"";
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getMessage();
    }

    /** 기동 시 대상 표 메타데이터: 컬럼(소문자) → 타입, PK. */
    static Map<String, String> targetColumns(JdbcTemplate jdbc, String table) {
        Map<String, String> out = new LinkedHashMap<>();
        TableId t = TableId.parse(table);
        jdbc.query("""
                SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
                 WHERE a.attrelid = to_regclass(?) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum
                """, rs -> {
            out.put(rs.getString(1), rs.getString(2));
        }, q(t));
        if (out.isEmpty()) {
            throw new IllegalStateException("대상 표 " + table + " 가 HotDB 에 없습니다 (db/legacy DDL 이 적용됐는지 확인)");
        }
        return out;
    }

    static List<String> targetPk(JdbcTemplate jdbc, String table) {
        return jdbc.queryForList("""
                SELECT a.attname FROM pg_index i
                  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
                 WHERE i.indrelid = to_regclass(?) AND i.indisprimary
                 ORDER BY array_position(i.indkey::int[], a.attnum::int)
                """, String.class, q(TableId.parse(table)));
    }
}
