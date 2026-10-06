package dev.hotdb.zone;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 실적 판별 모듈 — tsdb 필드 데이터를 CDC 로 받아 자기 모듈 RDB(svc_*)에 저장한다.
 *
 * <p>코드는 cdc-core 가 다 들고 있고, 이 서비스는 설정(application.yml 의 hotdb.routes)만 가진다.
 * 모듈은 실행 시 ZONE 환경변수(mch · asm · oft · pnt …)로 고른다 — 같은 이미지를 모듈 수만큼 띄운다.
 * 모듈은 DB 의 ops.module 에 등록돼 있어야 한다 (ops.provision_module).
 */
@SpringBootApplication
public class ZoneServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ZoneServiceApplication.class, args);
    }

    /** 권역 코드가 비었거나 등록부와 다르면 아무 장비도 안 받으며 조용히 돈다 — 기동에서 막는다. */
    @Bean
    ApplicationRunner moduleRegistrationCheck(JdbcTemplate jdbc, @Value("${ZONE:asm}") String module,
                                              @Value("${hotdb.zone.code:}") String zoneCode) {
        return args -> {
            if (zoneCode.isBlank()) {
                throw new IllegalStateException("모듈 " + module + " 의 권역 코드가 없습니다 — ZONE_CODE 또는 application-"
                        + module + ".yml 의 hotdb.zone.code");
            }
            String registered = jdbc.query("SELECT zone_code FROM ops.module WHERE module = ?",
                    rs -> rs.next() ? rs.getString(1) : null, module);
            if (registered == null) {
                throw new IllegalStateException("모듈 " + module + " 가 ops.module 에 없습니다 — "
                        + "V 파일에 SELECT ops.provision_module('" + module + "', '" + zoneCode + "', '이름');");
            }
            if (!registered.equals(zoneCode)) {
                throw new IllegalStateException("모듈 " + module + " 의 권역 코드가 등록부(" + registered + ")와 설정("
                        + zoneCode + ")이 다릅니다");
            }
        };
    }
}
