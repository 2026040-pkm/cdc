package dev.hotdb.cdc.resource;

import java.time.ZoneId;
import java.util.TimeZone;

/**
 * 현장 시간대를 JVM 기본값으로 둔다 — 기동 첫 줄에서 부른다.
 *
 * <p>PostgreSQL · Oracle JDBC 는 세션 시간대를 JVM 기본값으로 정한다. 컨테이너 기본(UTC)으로 두면
 * {@code current_date} 가 한국 날짜보다 9시간 늦게 바뀌고(판별 기준일이 전날로 들어감), 레거시의 시각 문자열
 * ({@code to_timestamp(mnt_date || mnt_time)})이 UTC 로 읽혀 9시간 밀린다 (2026-10-26 로컬 밤샘 시험에서 발견).
 * 값은 {@code HOTDB_SITE_TZ} (환경변수 · 시스템 속성), 기본 Asia/Seoul.
 */
public final class SiteZone {

    public static final String DEFAULT = "Asia/Seoul";

    private SiteZone() {}

    public static ZoneId apply() {
        String id = System.getProperty("HOTDB_SITE_TZ", System.getenv().getOrDefault("HOTDB_SITE_TZ", DEFAULT));
        ZoneId zone = ZoneId.of(id);
        TimeZone.setDefault(TimeZone.getTimeZone(zone));
        System.setProperty("user.timezone", zone.getId());
        return zone;
    }
}
