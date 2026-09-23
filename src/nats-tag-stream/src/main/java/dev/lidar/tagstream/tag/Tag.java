package dev.lidar.tagstream.tag;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Engine 이 보낸 태그 항목 하나를 클라이언트가 바로 쓸 수 있게 편 것.
 *
 * TagId 는 {@code {id}.{MQTT 토픽의 / 를 _ 로}.{필드}} 이다. 장비 · 토픽 · 채널을 따로 담은 칸이 없어서
 * 여기서 잘라 둔다. {@code value} 는 원래 raw_payload 를 한 번 더 감싼 JSON 문자열인데, 풀어서 객체로 싣는다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Tag(
        String tagId,
        String device,        // LDR-GJ-A1B2-01 — 산출물의 종류 접미사를 뗀 장비 id
        String artifactType,  // 산출물만: REGISTERED_PCD · TRANSFORMATION_MATRIX · SEGMENTED_PCD
        String topicKey,      // ot_pipeline_assembly_assembly1_bay2_artifact
        String channel,       // 토픽의 마지막 마디: status · actual · artifact
        String field,         // raw_payload
        String valueKind,
        String changedAt,     // raw_payload.occurred_at 과 같은 시각 (UTC). 도착 순서가 아니다
        JsonNode value) {
}
