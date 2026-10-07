package dev.hotdb.capture;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * 캡처부 변경 로그 ({@code hotdb.capture.*}).
 *
 * @param logDir       세그먼트 · ack 파일 자리 (캡처부 볼륨)
 * @param segmentSize  세그먼트 하나 크기 — 지우는 단위
 * @param maxSize      이만큼 넘게 쌓이면 덧붙이기를 막아 슬롯이 WAL 을 쥐게 한다 (전달부가 오래 죽었을 때 디스크 상한)
 * @param consumers    보존 기준에 드는 전달부 이름. 여기 있는 소비자가 ack 하기 전에는 지우지 않는다 — 멈춘 전달부 몫도 남는다
 */
@ConfigurationProperties("hotdb.capture")
public record CaptureProperties(String logDir,
                                @DefaultValue("64MB") DataSize segmentSize,
                                @DefaultValue("2GB") DataSize maxSize,
                                List<String> consumers) {

    @Override
    public List<String> consumers() {
        return consumers == null ? List.of() : consumers;
    }
}
