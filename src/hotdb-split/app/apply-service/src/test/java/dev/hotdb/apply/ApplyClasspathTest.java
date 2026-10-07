package dev.hotdb.apply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** 경계 — 전달부는 엔진을 모른다. Debezium 을 바꾸거나 걷어내도 전달부는 다시 빌드할 일이 없다. */
class ApplyClasspathTest {

    @Test
    void Debezium_이_클래스패스에_없다() {
        assertThatThrownBy(() -> Class.forName("io.debezium.engine.DebeziumEngine"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void 판별_모듈_코드는_zone_service_와_같다() throws Exception {
        assertThat(Class.forName("dev.hotdb.zone.ZoneServiceApplication")).isNotNull();
        assertThat(Class.forName("dev.hotdb.cdc.log.LogPullSource")).isNotNull();
    }
}
