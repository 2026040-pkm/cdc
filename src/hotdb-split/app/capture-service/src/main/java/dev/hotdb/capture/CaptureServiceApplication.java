package dev.hotdb.capture;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hotdb.cdc.log.ChangeRecordCodec;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * 캡처부 — 슬롯을 읽어(cdc-debezium) 변경 로그에 쓰고, /log 로 내준다.
 *
 * <p>책임: 슬롯 · 슬롯 연속성(ops.cdc_checkpoint) · 엔진 오프셋 · 변경 로그 보존.
 * 책임 아님: 라우트 · 반영 · 판별 · dead letter 재처리 — 전부 전달부(apply-service).
 */
@SpringBootApplication
@EnableConfigurationProperties(CaptureProperties.class)
public class CaptureServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CaptureServiceApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    FileChangeLog fileChangeLog(CaptureProperties props, MeterRegistry meters) throws IOException {
        return new FileChangeLog(Path.of(props.logDir()), props.segmentSize().toBytes(), props.maxSize().toBytes(),
                props.consumers(), meters);
    }

    /** 이 빈이 있어서 cdc-core 의 라우트 반영기(RouteApplier)는 만들어지지 않는다 */
    @Bean
    LogAppendingSink logAppendingSink(FileChangeLog log, ObjectMapper json) {
        return new LogAppendingSink(log, new ChangeRecordCodec(json));
    }
}
