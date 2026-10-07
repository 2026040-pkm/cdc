package dev.hotdb.rfc;

import dev.hotdb.cdc.HotdbCdcProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * RFC Service (O-8) — 판별 모듈 RDB(svc.actual_result)를 CDC 로 받아 SAP 로 보내고 (2단 CDC),
 * SAP 레거시 표를 폴링해 레거시 DB erp 에 쓴다 (poll-core — 설정 hotdb.poll).
 *
 * <pre>
 * Provider → [tsdb] ─CDC─▶ 판별 모듈 → [svc] ─CDC─▶ RFC Service → SAP
 * </pre>
 *
 * <p>엔진 · 디코딩은 cdc-core 를 그대로 쓰고, 반영 대상만 표가 아니라 {@link ActualResultRelay} 로 바꾼다.
 * SAP 송신은 {@link RfcSender} — dry-run · jdbc(Z 테이블). JCo 가 붙으면 구현 하나만 더한다.
 */
@SpringBootApplication
@EnableConfigurationProperties(RfcProperties.class)
public class RfcServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(RfcServiceApplication.class, args);
    }

    /** SAP 송신 방식 — hotdb.rfc.sender. 나머지(CDC · 중복 차단 · 재시도)는 방식과 상관없이 같다. */
    @Bean
    RfcSender rfcSender(RfcProperties rfc) {
        return switch (rfc.sender()) {
            case "dry-run" -> new DryRunRfcSender();
            case "jdbc" -> {
                var ds = new com.zaxxer.hikari.HikariDataSource();
                ds.setPoolName("sap-sender");
                ds.setJdbcUrl(rfc.sap().url());
                ds.setUsername(rfc.sap().user());
                ds.setPassword(rfc.sap().password());
                ds.setMaximumPoolSize(2);
                ds.setInitializationFailTimeout(-1);   // SAP 가 늦게 떠도 서비스는 뜬다 — 송신 때 재시도
                yield new JdbcRfcSender(new JdbcTemplate(ds), rfc.sap().table());
            }
            default -> throw new IllegalStateException("hotdb.rfc.sender 는 dry-run · jdbc — " + rfc.sender());
        };
    }

    @Bean
    ActualResultRelay actualResultRelay(HotdbCdcProperties cdc, RfcProperties rfc, JdbcTemplate jdbc,
                                        TransactionTemplate tx, RfcSender sender, MeterRegistry meters) {
        return new ActualResultRelay(cdc.pipeline(), rfc, jdbc, tx, sender, meters);
    }
}
