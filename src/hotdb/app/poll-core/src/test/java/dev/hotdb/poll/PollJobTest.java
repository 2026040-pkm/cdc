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
    void 작업_묶음은_기본_목록과_합쳐진다() {
        PollProperties.JobSpec a = new PollProperties.JobSpec(null, "sap", "erpsrc.item", "erp.item", null, null, "incremental", "w", null, true,
                null, false);
        assertThat(a.trackScope()).as("scope 를 비우면 작업 이름").isEqualTo("erp.item");
        assertThat(a.markMissing()).isFalse();
        PollProperties.JobSpec r = new PollProperties.JobSpec("hot.l_wo_daily.resync", "legacy", "(SELECT 1) q", "hot.l_wo_daily",
                "hot", null, "full", null, 86_400_000L, true, "hot.l_wo_daily", true);
        assertThat(r.trackScope()).as("재동기화 작업은 증분 작업과 같은 범위").isEqualTo("hot.l_wo_daily");
        PollProperties.JobSpec b = new PollProperties.JobSpec(null, "legacy", "(SELECT 1) q", "hot.m_hull", "hot", null, "full", null, null, true,
                null, false);
        PollProperties p = new PollProperties("rfc-service", null, null, null, List.of(a), java.util.Map.of("hot", List.of(b)), 60000, 1000, 500, 2);
        assertThat(p.jobs()).extracting(PollProperties.JobSpec::jobName).containsExactly("erp.item", "hot.m_hull");
    }

    @Test
    void 원천_타입은_문자열로_바꿔_대상에서_CAST_한다() throws Exception {
        assertThat(PollValues.toSql(new BigDecimal("12.500"))).isEqualTo("12.5");
        assertThat(PollValues.toSql(new BigDecimal("1E+3"))).isEqualTo("1000");
        assertThat(PollValues.toSql(Timestamp.valueOf(LocalDateTime.of(2026, 10, 6, 9, 30)))).isEqualTo("2026-10-06T09:30");
        assertThat(PollValues.toSql(null)).isNull();
    }
}
