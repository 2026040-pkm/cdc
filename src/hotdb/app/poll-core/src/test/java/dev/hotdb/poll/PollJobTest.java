package dev.hotdb.poll;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class PollJobTest {

    @Test
    void 증분_첫_주기는_전부_읽고_워터마크를_같이_뽑는다() {
        assertThat(PollJob.selectSql("MES.WORK_LOG", List.of("EQUIP_CODE", "UPD_DATE"), "upd_date || upd_time", false))
                .isEqualTo("SELECT \"EQUIP_CODE\", \"UPD_DATE\", (upd_date || upd_time) AS HOTDB_WM FROM MES.WORK_LOG");
    }

    @Test
    void 증분_다음_주기는_지난_워터마크_이상만() {
        assertThat(PollJob.selectSql("erpsrc.item", List.of("item_no", "item_name"), "upd_date || upd_time", true))
                .endsWith("FROM erpsrc.item WHERE (upd_date || upd_time) >= ?");
    }

    @Test
    void 전체_작업은_워터마크가_없다() {
        assertThat(PollJob.selectSql("t", List.of("a"), null, false)).isEqualTo("SELECT \"a\" FROM t");
    }

    @Test
    void 원천_타입은_문자열로_바꿔_대상에서_CAST_한다() throws Exception {
        assertThat(PollValues.toSql(new BigDecimal("12.500"))).isEqualTo("12.5");
        assertThat(PollValues.toSql(new BigDecimal("1E+3"))).isEqualTo("1000");
        assertThat(PollValues.toSql(Timestamp.valueOf(LocalDateTime.of(2026, 10, 6, 9, 30)))).isEqualTo("2026-10-06T09:30");
        assertThat(PollValues.toSql(null)).isNull();
    }
}
