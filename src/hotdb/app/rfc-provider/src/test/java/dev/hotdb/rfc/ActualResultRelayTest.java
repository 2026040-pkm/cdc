package dev.hotdb.rfc;

import static org.assertj.core.api.Assertions.assertThat;

import dev.hotdb.cdc.event.TableId;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

class ActualResultRelayTest {

    final ActualResultRelay relay = new ActualResultRelay("rfc-provider", new RfcProperties(0, 0, "dry-run", null), null, null,
            new DryRunRfcSender(), new SimpleMeterRegistry());

    @Test
    void svc_의_실적_표만_받는다() {
        assertThat(relay.interestedIn(new TableId("svc", "actual_result"))).isTrue();
        assertThat(relay.interestedIn(new TableId("svc_asm", "actual_result"))).as("V9 전의 모듈 스키마").isFalse();
        assertThat(relay.interestedIn(new TableId("svc", "device_status_current"))).isFalse();
        assertThat(relay.interestedIn(new TableId("ops", "cdc_heartbeat"))).isFalse();
        assertThat(relay.interestedIn(new TableId("tsdb", "actual_result"))).isFalse();
    }
}
