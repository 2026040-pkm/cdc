package dev.lidar.tagstream.ws;

import dev.lidar.tagstream.tag.Tag;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 접속 URL 의 쿼리로 받는 구독 조건. 둘 다 비우면 전부 받는다.
 *
 * <pre>
 * /ws/tags?channel=status,actual          채널 (status · actual · artifact)
 * /ws/tags?device=LDR-GJ-A1B2,LDR-GJ-O1   장비 id 앞부분 — LDR-GJ-A1B2 는 조립 1공장 2베이 전체
 * </pre>
 *
 * record 라 같은 조건끼리 equals 가 같다. 같은 조건의 세션은 직렬화 결과를 나눠 쓴다.
 */
public record TagFilter(Set<String> channels, List<String> devicePrefixes) {

    public static final TagFilter ALL = new TagFilter(Set.of(), List.of());

    public static TagFilter from(URI uri) {
        if (uri == null) {
            return ALL;
        }
        var params = UriComponentsBuilder.fromUri(uri).build().getQueryParams();
        return new TagFilter(
                Set.copyOf(split(params.getOrDefault("channel", List.of()))),
                split(params.getOrDefault("device", List.of())).stream().sorted().toList());
    }

    private static List<String> split(List<String> values) {
        return values.stream()
                .flatMap(v -> Arrays.stream(v.split(",")))
                .map(String::strip)
                .filter(s -> !s.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    public boolean isAll() {
        return channels.isEmpty() && devicePrefixes.isEmpty();
    }

    public boolean test(Tag tag) {
        if (!channels.isEmpty() && !channels.contains(tag.channel())) {
            return false;
        }
        return devicePrefixes.isEmpty()
                || devicePrefixes.stream().anyMatch(p -> tag.device().startsWith(p));
    }
}
