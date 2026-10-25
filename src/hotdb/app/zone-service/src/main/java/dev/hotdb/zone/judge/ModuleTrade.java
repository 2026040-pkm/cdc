package dev.hotdb.zone.judge;

import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 모듈 ↔ 공종 ↔ 앵커 유형. ops.module.trade (V17) 로 읽는다 — C 가공 · S 조립 · W 선행의장 · P 선행도장 (= YPWG411M.FLAG).
 *
 * @param module     mch · asm · oft · pnt
 * @param trade      m_code TRADE
 * @param anchorType 이 공종의 앵커 (l_work_unit.anchor_type)
 * @param stage      p_progress_block.stage 에 적는 단계 이름
 */
public record ModuleTrade(String module, String trade, String anchorType, String stage) {

    private static final Map<String, String> ANCHOR = Map.of("C", "DWG", "S", "WSTG", "W", "GDO", "P", "PAINT_ORDER");
    private static final Map<String, String> STAGE = Map.of("C", "CUT", "S", "ASSY", "W", "OUTFIT", "P", "PAINT");

    static ModuleTrade load(JdbcTemplate jdbc, String module) {
        String trade = jdbc.query("SELECT trade FROM ops.module WHERE module = ?", rs -> rs.next() ? rs.getString(1) : null, module);
        if (trade == null || !ANCHOR.containsKey(trade)) {
            throw new IllegalStateException("모듈 " + module + " 의 공종(ops.module.trade)이 없거나 모르는 값: " + trade
                    + " — C · S · W · P 중 하나여야 한다 (V17)");
        }
        return new ModuleTrade(module, trade, ANCHOR.get(trade), STAGE.get(trade));
    }
}
