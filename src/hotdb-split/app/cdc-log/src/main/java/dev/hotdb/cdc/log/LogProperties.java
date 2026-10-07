package dev.hotdb.cdc.log;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 전달부가 캡처부 로그를 당겨 오는 설정 ({@code hotdb.log.*}, {@code hotdb.cdc.adapter=log} 일 때).
 *
 * @param captureUrl 캡처부 주소 (http://capture-asm:8080)
 * @param consumer   이 전달부의 이름 — 캡처부 보존 기준(ack)에 찍힌다. 캡처부 {@code hotdb.capture.consumers} 에 있어야
 *                   전달부가 멈춘 동안에도 로그가 지워지지 않는다
 * @param offsetFile 이 전달부가 어디까지 반영했는지 (순번 하나). 전달부 볼륨에 둔다
 * @param maxRecords 한 번에 당겨 오는 최대 건수 = 반영 배치 크기
 * @param waitMs     새 줄이 없을 때 캡처부가 응답을 붙잡아 두는 시간 (long poll)
 * @param retryMs    캡처부에 못 붙을 때 다시 시도하는 간격
 */
@ConfigurationProperties("hotdb.log")
public record LogProperties(String captureUrl, String consumer, String offsetFile,
                            @DefaultValue("2048") int maxRecords,
                            @DefaultValue("1000") long waitMs,
                            @DefaultValue("1000") long retryMs) {}
