package dev.lidar.tagstream.tag;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.lidar.tagstream.ws.TagFilter;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class TagParserTest {

    private final TagParser parser = new TagParser(new ObjectMapper());

    // Engine 이 실제로 보내는 모양 그대로 — value 안의 따옴표가 " 로 이스케이프돼 있다
    private static final String MESSAGE = """
            {"edgeGroupId":"edge.mqtt.p3.ot","tags":[
             {"tagId":"LDR-GJ-O1B5-15.ot_device_outfitting_lidar_status.raw_payload","valueKind":"String",
              "value":"{\\u0022status\\u0022:\\u0022ONLINE\\u0022,\\u0022occurred_at\\u0022:\\u00222026-09-22T09:26:20.4189515\\u002B09:00\\u0022}",
              "changedAt":"2026-09-22T00:26:20.4189515+00:00"},
             {"tagId":"LDR-GJ-O1B6-17.ot_sensor_piping_actual.raw_payload","valueKind":"String",
              "value":"{\\u0022stage\\u0022:\\u0022PIPING\\u0022,\\u0022event_type\\u0022:\\u0022START\\u0022}",
              "changedAt":"2026-09-22T00:26:20.4342298+00:00"},
             {"tagId":"LDR-GJ-O1B6-12-SEGMENTED_PCD.ot_pipeline_outfitting_outfitting1_bay6_artifact.raw_payload",
              "valueKind":"String","value":"{\\u0022segment_id\\u0022:\\u0022SEG-003\\u0022}",
              "changedAt":"2026-09-22T00:26:19.5830205+00:00"},
             {"tagId":"LDR-GJ-A1B1-01.ot_device_assembly_lidar_status.temperature_c","valueKind":"Double",
              "value":37.5,"changedAt":"2026-09-22T00:26:20+00:00"}
            ]}""";

    @Test
    void 봉투와_value_를_두_번_풀어_태그로_편다() throws Exception {
        TagBatch batch = parser.parse("s", MESSAGE.getBytes(StandardCharsets.UTF_8), Instant.EPOCH);

        assertThat(batch.edgeGroupId()).isEqualTo("edge.mqtt.p3.ot");
        assertThat(batch.tags()).hasSize(4);

        Tag status = batch.tags().get(0);
        assertThat(status.device()).isEqualTo("LDR-GJ-O1B5-15");
        assertThat(status.artifactType()).isNull();
        assertThat(status.topicKey()).isEqualTo("ot_device_outfitting_lidar_status");
        assertThat(status.channel()).isEqualTo("status");
        assertThat(status.field()).isEqualTo("raw_payload");
        assertThat(status.value().path("status").asText()).isEqualTo("ONLINE");
        assertThat(status.value().path("occurred_at").asText()).isEqualTo("2026-09-22T09:26:20.4189515+09:00");

        Tag actual = batch.tags().get(1);
        assertThat(actual.channel()).isEqualTo("actual");
        assertThat(actual.value().path("stage").asText()).isEqualTo("PIPING");

        Tag segment = batch.tags().get(2);
        assertThat(segment.device()).isEqualTo("LDR-GJ-O1B6-12");
        assertThat(segment.artifactType()).isEqualTo("SEGMENTED_PCD");
        assertThat(segment.channel()).isEqualTo("artifact");

        // tagMode=fields 의 숫자 태그는 그대로 둔다
        Tag numeric = batch.tags().get(3);
        assertThat(numeric.value().asDouble()).isEqualTo(37.5);
        assertThat(numeric.field()).isEqualTo("temperature_c");
    }

    @Test
    void 필터는_채널과_장비_앞부분으로_거른다() throws Exception {
        TagBatch batch = parser.parse("s", MESSAGE.getBytes(StandardCharsets.UTF_8), Instant.EPOCH);

        TagFilter filter = TagFilter.from(URI.create("ws://h/ws/tags?channel=status,artifact&device=LDR-GJ-O1"));

        assertThat(batch.tags().stream().filter(filter::test).map(Tag::device))
                .containsExactly("LDR-GJ-O1B5-15", "LDR-GJ-O1B6-12");
        assertThat(TagFilter.from(URI.create("ws://h/ws/tags")).isAll()).isTrue();
    }
}
