package dev.hotdb.migrate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Flyway 마이그레이션을 한 번 돌리고 끝나는 실행기.
 *
 * <p>Hot DB 는 {@code db/migration}, 레거시 DB 는 {@code db/legacy-db} 를 읽는다
 * (위치는 HOTDB_MIGRATION_LOCATIONS). 스키마를 바꿀 때는 기존 V 파일을 고치지 말고 새 V 파일을 더한다 —
 * 이미 적용된 파일의 체크섬이 바뀌면 Flyway 가 기동을 거부한다.
 */
@SpringBootApplication
public class HotdbMigrateApplication {
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(HotdbMigrateApplication.class, args)));
    }
}
