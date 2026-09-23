package dev.lidar.tagstream.tag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Engine 메시지 {@code {"edgeGroupId", "tags":[{tagId, valueKind, value, changedAt}]}} 를 푼다.
 *
 * value 는 JSON 안의 JSON 문자열이라(.NET 이 따옴표를 유니코드 이스케이프 u0022 로 바꿔 둔다) 두 번 파싱해야 원문이 나온다.
 * 객체 · 배열이 아니면(tagMode=fields 의 숫자 태그 등) 문자열 그대로 둔다.
 */
@Component
public class TagParser {

    // 산출물 채널은 장비 하나가 종류마다 다른 id 를 쓴다: LDR-GJ-A1B2-01-SEGMENTED_PCD
    private static final Pattern ARTIFACT_ID = Pattern.compile("^(.+-\\d+)-([A-Z][A-Z_]+)$");

    private final ObjectMapper mapper;

    public TagParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public TagBatch parse(String subject, byte[] data, Instant receivedAt) throws IOException {
        JsonNode root = mapper.readTree(data);
        JsonNode items = root.path("tags");
        List<Tag> tags = new ArrayList<>(items.size());
        for (JsonNode item : items) {
            tags.add(toTag(item));
        }
        return new TagBatch(subject, root.path("edgeGroupId").asText(null), receivedAt, tags);
    }

    Tag toTag(JsonNode item) {
        String tagId = item.path("tagId").asText();
        int first = tagId.indexOf('.');
        int last = tagId.lastIndexOf('.');

        String id = first < 0 ? tagId : tagId.substring(0, first);
        String topicKey = first < 0 || last <= first ? null : tagId.substring(first + 1, last);
        String field = last < 0 ? null : tagId.substring(last + 1);
        String channel = topicKey == null ? null : topicKey.substring(topicKey.lastIndexOf('_') + 1);

        String device = id;
        String artifactType = null;
        Matcher m = ARTIFACT_ID.matcher(id);
        if (m.matches()) {
            device = m.group(1);
            artifactType = m.group(2);
        }

        return new Tag(tagId, device, artifactType, topicKey, channel, field,
                item.path("valueKind").asText(null),
                item.path("changedAt").asText(null),
                unwrap(item.path("value")));
    }

    private JsonNode unwrap(JsonNode value) {
        if (!value.isTextual()) {
            return value;
        }
        String text = value.asText().strip();
        if (text.startsWith("{") || text.startsWith("[")) {
            try {
                return mapper.readTree(text);
            } catch (IOException e) {
                // 원문이 깨져 있으면 버리지 않고 문자열로 넘긴다 — 보는 쪽에서 무엇이 왔는지 알 수 있게
                return TextNode.valueOf(value.asText());
            }
        }
        return value;
    }
}
