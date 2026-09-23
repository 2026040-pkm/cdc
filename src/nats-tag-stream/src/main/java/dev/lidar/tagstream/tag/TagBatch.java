package dev.lidar.tagstream.tag;

import java.time.Instant;
import java.util.List;

/** NATS 메시지 하나 = Engine 이 약 1초 동안 모은 태그 변경 묶음. */
public record TagBatch(String subject, String edgeGroupId, Instant receivedAt, List<Tag> tags) {
}
