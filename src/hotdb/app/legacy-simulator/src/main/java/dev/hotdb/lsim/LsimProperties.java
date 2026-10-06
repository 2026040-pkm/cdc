package dev.hotdb.lsim;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param intervalMs       한 주기 (기본 10초). 주기마다 표마다 updates · inserts 만큼 바꾼다
 * @param speed            배속. 2 면 주기가 절반
 * @param updatesPerTable  주기마다 표마다 고칠 행 수 (upd_date · upd_time = 지금, 글자 컬럼 하나 = CHG-시각)
 * @param insertsPerTable  주기마다 표마다 새로 넣을 행 수 (PK 는 LSIM + 일련번호)
 * @param autoStart        기동하자마자 돌릴지 (false 면 POST /sim/resume)
 * @param sap              SAP 대역 — PostgreSQL, 스키마 하나, 표 목록 (비면 Z 로 시작하지 않는 표 전부)
 * @param oracle           Oracle 대역 — 소유자(스키마) 목록, 표 전부
 */
@ConfigurationProperties("hotdb.lsim")
public record LsimProperties(
        @DefaultValue("10000") long intervalMs,
        @DefaultValue("1.0") double speed,
        @DefaultValue("3") int updatesPerTable,
        @DefaultValue("1") int insertsPerTable,
        @DefaultValue("true") boolean autoStart,
        Sap sap,
        Oracle oracle) {

    public record Sap(String url, String user, String password, @DefaultValue("erpsrc") String schema, List<String> tables) {}

    public record Oracle(String url, String user, String password, @DefaultValue({"MES", "LGS", "GEO"}) List<String> schemas) {}
}
