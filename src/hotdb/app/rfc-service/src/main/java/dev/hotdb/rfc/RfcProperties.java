package dev.hotdb.rfc;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * SAP 송신 정책 ({@code hotdb.rfc.*}).
 *
 * @param maxRetries SAP · DB 일시 장애 재시도 횟수. 소진하면 예외를 올려 프로세스를 재시작시킨다 —
 *                   오프셋이 안 넘어가 슬롯이 WAL 을 붙잡고, 살아나면 그 자리부터 다시 보낸다
 * @param backoffMs  첫 재시도 대기 (시도마다 두 배)
 * @param sender     dry-run (로그만) · jdbc (SAP Z 테이블에 JDBC INSERT). JCo 가 생기면 jco 를 더한다
 * @param sap        jdbc 송신 대상 — 운영은 HANA(jdbc:sap://…), 로컬은 SAP 대역 DB
 */
@ConfigurationProperties("hotdb.rfc")
public record RfcProperties(@DefaultValue("5") int maxRetries, @DefaultValue("500") long backoffMs,
                            @DefaultValue("dry-run") String sender, Sap sap) {

    /**
     * @param table SAP 쪽 실적 표 (스키마.표). PK = zone, hull_no, block_id, scan_id, judged_status
     */
    public record Sap(String url, String user, String password, @DefaultValue("zhotdb_actual_result") String table) {}
}
