package dev.hotdb.sim;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param devices          권역 코드 → 장비 수. 카탈로그 기준 조립 210 · 의장 140
 * @param statusIntervalMs status 주기 (1초)
 * @param actualIntervalMs actual · artifact 주기 (1분)
 * @param speed            배속. 2 면 주기가 절반 — 행/초가 두 배
 * @param anomalyRate      status 한 건이 ERROR/CALIBRATING 으로 튈 확률 (상태 전이 이력이 생기게)
 * @param artifacts        actual 마다 산출물 12건을 같이 낼지
 * @param autoStart        기동하자마자 발행할지 (false 면 POST /sim/resume 으로 시작)
 */
@ConfigurationProperties("hotdb.sim")
public record SimProperties(
        @DefaultValue("geoje") String site,
        Map<String, Integer> devices,
        @DefaultValue("1000") long statusIntervalMs,
        @DefaultValue("60000") long actualIntervalMs,
        @DefaultValue("1.0") double speed,
        @DefaultValue("0.002") double anomalyRate,
        @DefaultValue("true") boolean artifacts,
        @DefaultValue("true") boolean autoStart) {}
