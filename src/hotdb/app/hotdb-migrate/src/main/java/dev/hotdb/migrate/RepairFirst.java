package dev.hotdb.migrate;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * HOTDB_FLYWAY_REPAIR=true 이면 migrate 전에 repair 한다 — 이미 적용한 파일의 주석만 고쳤을 때
 * 체크섬을 다시 맞추고, 사라진 파일의 이력을 지운다. 스키마 자체를 바꾸려면 새 V 파일을 더한다.
 */
@Configuration
class RepairFirst {

    @Bean
    FlywayMigrationStrategy migrationStrategy(@Value("${HOTDB_FLYWAY_REPAIR:false}") boolean repair) {
        return flyway -> {
            if (repair) {
                flyway.repair();
            }
            flyway.migrate();
        };
    }
}
