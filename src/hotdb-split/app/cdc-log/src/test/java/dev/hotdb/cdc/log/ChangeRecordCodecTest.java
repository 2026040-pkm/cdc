package dev.hotdb.cdc.log;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ChangeRecordCodecTest {

    final ChangeRecordCodec codec = new ChangeRecordCodec(new ObjectMapper());

    @Test
    void 캡처부가_쓴_줄을_전달부가_같은_이벤트로_읽는다() {
        Map<String, Object> after = new LinkedHashMap<>();
        after.put("tag_key", 7);
        after.put("status", "ONLINE");
        after.put("temperature_c", 35.5);
        after.put("transformation_matrix", List.of(1.0, 0.02));
        after.put("error_code", null);
        CdcEvent e = new CdcEvent(TableId.parse("tsdb.status_history"),
                new TableId("_timescaledb_internal", "_hyper_1_9_chunk"), Op.CREATE, null, after,
                2483194880L, 1791339636123L, "{debezium 원문}");

        ChangeRecord r = codec.decodeLine("42\t" + codec.encode("lsn:2483194880", e));

        assertThat(r.seq()).isEqualTo(42);
        assertThat(r.position()).isEqualTo("lsn:2483194880");
        assertThat(r.event().table()).isEqualTo(e.table());
        assertThat(r.event().physical()).isEqualTo(e.physical());
        assertThat(r.event().op()).isEqualTo(Op.CREATE);
        assertThat(r.event().after()).isEqualTo(after);
        assertThat(r.event().before()).isNull();
        assertThat(r.event().lsn()).isEqualTo(2483194880L);
        assertThat(r.event().commitTsMs()).isEqualTo(1791339636123L);
    }

    @Test
    void 원문은_엔진_형식이_아니라_로그_줄이다_dead_letter_재처리가_엔진을_모른다() {
        CdcEvent e = new CdcEvent(TableId.parse("tsdb.device"), TableId.parse("tsdb.device"), Op.DELETE,
                Map.of("device_id", "LDR-1"), null, null, null, "ignored");
        String body = codec.encode(null, e);

        CdcEvent back = codec.decodeLine("1\t" + body).event();

        assertThat(back.raw()).isEqualTo(body);
        assertThat(codec.decodeJson(back.raw()).before()).containsEntry("device_id", "LDR-1");
        assertThat(back.row()).containsEntry("device_id", "LDR-1");
    }

    @Test
    void 모르는_필드는_무시한다_계약은_더하기만_한다() {
        CdcEvent e = codec.decodeJson("{\"table\":\"tsdb.device\",\"op\":\"u\",\"after\":{\"a\":1},\"tx\":\"new-field\"}");

        assertThat(e.op()).isEqualTo(Op.UPDATE);
        assertThat(e.physical()).isEqualTo(e.table());
    }
}
