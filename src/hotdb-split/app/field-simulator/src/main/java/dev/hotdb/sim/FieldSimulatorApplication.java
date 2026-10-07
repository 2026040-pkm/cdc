package dev.hotdb.sim;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * 필드 데이터 발행기. HotDB Provider 자리에서 tsdb 에 직접 적재한다 (계정 hotdb_provider).
 *
 * <p>태그 카탈로그(docs/backup/latest/mqtt-agent-tag-catalog.html) 기준 3채널 · 350대를 흉내 낸다.
 * MQTT · MQTT Agent · Provider 를 빼고 tsdb 만 채우므로, CDC 쪽 부하를 따로 떼어 잴 수 있다.
 * 배속은 실행 중에 {@code POST /sim/speed?value=5} 로 바꾼다.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class FieldSimulatorApplication {
    public static void main(String[] args) {
        SpringApplication.run(FieldSimulatorApplication.class, args);
    }
}
