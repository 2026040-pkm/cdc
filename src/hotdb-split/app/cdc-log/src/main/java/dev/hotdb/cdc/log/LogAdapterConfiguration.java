package dev.hotdb.cdc.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hotdb.cdc.HotdbCdcAutoConfiguration;
import dev.hotdb.cdc.HotdbCdcProperties;
import dev.hotdb.cdc.port.CdcSink;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** {@code hotdb.cdc.adapter=log} 이면 캡처부 변경 로그를 당겨 오는 {@link LogPullSource} 를 원천으로 둔다. */
@AutoConfiguration(after = HotdbCdcAutoConfiguration.class)
@ConditionalOnProperty(name = "hotdb.cdc.adapter", havingValue = "log")
@EnableConfigurationProperties(LogProperties.class)
public class LogAdapterConfiguration {

    @Bean
    LogPullSource logPullSource(HotdbCdcProperties cdc, LogProperties props, CdcSink sink, ObjectMapper json,
                                MeterRegistry meters) {
        return new LogPullSource(cdc, props, sink, json, meters);
    }
}
