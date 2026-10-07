package dev.hotdb.rfc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 통합 테스트용 얇은 JDBC 도우미. 기동 중인 HotDB(-Dhotdb.jdbc.url)에 계정별로 붙는다. */
final class Db implements AutoCloseable {

    static final String URL = System.getProperty("hotdb.jdbc.url", "jdbc:postgresql://localhost:59433/hotdb");

    private final Connection conn;

    private Db(String url, String user) throws SQLException {
        this.conn = DriverManager.getConnection(url, user, user.equals("postgres") ? "postgres" : user);
    }

    static Db as(String user) throws SQLException {
        return new Db(URL, user);
    }

    /** Hot DB 가 아닌 다른 DB(레거시 DB 등)에 붙는다. */
    static Db at(String url, String user) throws SQLException {
        return new Db(url, user);
    }

    int update(String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = prepare(sql, args)) {
            return ps.executeUpdate();
        }
    }

    List<Map<String, Object>> rows(String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = prepare(sql, args); ResultSet rs = ps.executeQuery()) {
            List<Map<String, Object>> out = new ArrayList<>();
            var md = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    row.put(md.getColumnLabel(i), rs.getObject(i));
                }
                out.add(row);
            }
            return out;
        }
    }

    Object one(String sql, Object... args) throws SQLException {
        List<Map<String, Object>> r = rows(sql, args);
        return r.isEmpty() ? null : r.get(0).values().iterator().next();
    }

    long count(String sql, Object... args) throws SQLException {
        Object v = one(sql, args);
        return v == null ? 0 : ((Number) v).longValue();
    }

    private PreparedStatement prepare(String sql, Object... args) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 0; i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
        return ps;
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }
}
