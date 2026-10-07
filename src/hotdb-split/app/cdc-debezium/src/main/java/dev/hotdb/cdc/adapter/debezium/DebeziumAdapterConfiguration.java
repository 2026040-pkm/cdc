package dev.hotdb.cdc.adapter.debezium;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hotdb.cdc.HotdbCdcProperties;
import dev.hotdb.cdc.apply.DeadLetters;
import dev.hotdb.cdc.continuity.CdcCheckpoints;
import dev.hotdb.cdc.continuity.SlotContinuityGuard;
import dev.hotdb.cdc.port.CdcSink;
import dev.hotdb.cdc.timescale.HypertableResolver;
import io.micrometer.core.instrument.MeterRegistry;
import dev.hotdb.cdc.HotdbCdcAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * {@code hotdb.cdc.adapter=debezium} (기본값) 이면 Debezium 어댑터를 {@link dev.hotdb.cdc.port.CdcSource} 로 둔다.
 * 이 모듈(cdc-debezium)을 의존성에 넣은 서비스만 켜진다 — cdc-core 는 엔진을 모른다.
 */
@AutoConfiguration(after = HotdbCdcAutoConfiguration.class)
@ConditionalOnProperty("hotdb.cdc.pipeline")
@ConditionalOnProperty(name = "hotdb.cdc.adapter", havingValue = "debezium", matchIfMissing = true)
public class DebeziumAdapterConfiguration {

    @Bean
    DebeziumEventDecoder debeziumEventDecoder(ObjectMapper json, HypertableResolver resolver) {
        return new DebeziumEventDecoder(json, resolver);
    }

    @Bean
    DebeziumCdcSource debeziumCdcSource(HotdbCdcProperties props, DebeziumEventDecoder decoder, CdcSink sink,
                                        DeadLetters deadLetters, CdcCheckpoints checkpoints,
                                        SlotContinuityGuard continuity, MeterRegistry meters) {
        return new DebeziumCdcSource(props, decoder, sink, deadLetters, checkpoints, continuity, meters);
    }
}
