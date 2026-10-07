package dev.hotdb.cdc.adapter.debezium;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.route.SqlValues;
import dev.hotdb.cdc.timescale.HypertableResolver;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DebeziumEventDecoderTest {

    static final TableId CHUNK = new TableId("_timescaledb_internal", "_hyper_3_12_chunk");
    static final TableId PARENT = TableId.parse("tsdb.artifact_history");

    final DebeziumEventDecoder decoder = new DebeziumEventDecoder(new ObjectMapper(),
            t -> t.equals(CHUNK) ? Optional.of(PARENT) : HypertableResolver.isChunk(t) ? Optional.empty() : Optional.of(t));

    @Test
    void 청크_이벤트는_하이퍼테이블_이름으로_바뀐다() {
        String json = """
                {"before":null,
                 "after":{"tag_key":7,"scan_id":"3f9a2b7e-6c1d-4e2a-9b3a-8d5c6a7b0e11",
                          "transformation_matrix":[1.0,0.02,-0.01],"event_time":"2026-10-06T01:00:00.123456Z"},
                 "source":{"schema":"_timescaledb_internal","table":"_hyper_3_12_chunk","lsn":123456,"ts_ms":1759712400000},
                 "op":"c","ts_ms":1759712400100}
                """;
        CdcEvent e = decoder.decode(json).event();

        assertThat(e.table()).isEqualTo(PARENT);
        assertThat(e.physical()).isEqualTo(CHUNK);
        assertThat(e.op()).isEqualTo(Op.CREATE);
        assertThat(e.lsn()).isEqualTo(123456L);
        assertThat(SqlValues.toSql(e.row().get("transformation_matrix"))).isEqualTo("{1.0,0.02,-0.01}");
        assertThat(e.row().get("event_time")).isEqualTo("2026-10-06T01:00:00.123456Z");
    }

    @Test
    void 압축_뭉치처럼_풀리지_않는_청크는_버린다() {
        String json = """
                {"after":{"x":1},"source":{"schema":"_timescaledb_internal","table":"_hyper_9_9_chunk"},"op":"c"}
                """;
        assertThat(decoder.decode(json).skipReason()).isEqualTo("unknown_chunk");
    }

    @Test
    void op_가_없는_레코드는_건너뛴다() {
        assertThat(decoder.decode("{\"ts_ms\":1}").skipReason()).isEqualTo("no_op");
        assertThat(decoder.decode(null).skipReason()).isEqualTo("tombstone");
    }

    @Test
    void 청크_이름_판정() {
        assertThat(HypertableResolver.isChunk(CHUNK)).isTrue();
        assertThat(HypertableResolver.isChunk(new TableId("_timescaledb_internal", "_hyper_1_1_chunk_compressed"))).isFalse();
        assertThat(HypertableResolver.isChunk(new TableId("_timescaledb_internal", "bgw_job_stat"))).isFalse();
        assertThat(HypertableResolver.isChunk(PARENT)).isFalse();
    }

    @Test
    void 배열_리터럴은_문자열을_따옴표로_감싼다() {
        assertThat(SqlValues.toSql(List.of("a", "b\"c"))).isEqualTo("{\"a\",\"b\\\"c\"}");
        assertThat(SqlValues.toSql(1.0E7)).isEqualTo("10000000");
    }
}
