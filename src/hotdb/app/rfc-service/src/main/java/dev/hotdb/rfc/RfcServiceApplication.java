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
 * RFC Service (O-8) — 세 방향.
 *
 * <pre>
 *   ① SAP 폴링            → 레거시 DB erp 사본                    (poll-core, poll-jobs.yml, 대상 기본)
 *   ② 레거시 사본 폴링    → Hot DB hot.m_* · l_* (통합안 · 정규화)  (poll-core, poll-jobs-hot.yml, 대상 hot)
 *   ③ hot.p_progress CDC  → SAP Z 표 zhotdb_progress               (2단 CDC, {@link ProgressRelay})
 *
 *   Provider → [tsdb] ─CDC─▶ 판별 모듈 → [hot.s_* · p_*] ─CDC─▶ RFC Service → SAP
 *   레거시(inshoplocal 사본 · 운영은 HANA) ─폴링─▶ RFC Service ─▶ [hot.l_* · m_*] ─▶ 판별 모듈이 앵커 · 절점으로 읽는다
 * </pre>
 *
 * <p>엔진 · 디코딩은 cdc-core 를 그대로 쓰고, 반영 대상만 표가 아니라 {@link ProgressRelay} 로 바꾼다.
 * SAP 송신은 {@link RfcSender} — dry-run · jdbc(Z 테이블). JCo 가 붙으면 구현 하나만 더한다.
 */
@SpringBootApplication
@EnableConfigurationProperties(RfcProperties.class)
public class RfcServiceApplication {

    public static void main(String[] args) {
        dev.hotdb.cdc.resource.SiteZone.apply();   // JDBC 세션 시간대 = 현장 시간대 (UTC 면 날짜 · 시각이 9시간 밀린다)
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
    ProgressRelay progressRelay(HotdbCdcProperties cdc, RfcProperties rfc, JdbcTemplate jdbc,
                                TransactionTemplate tx, RfcSender sender, MeterRegistry meters) {
        return new ProgressRelay(cdc.pipeline(), rfc, jdbc, tx, sender, meters);
    }
}
