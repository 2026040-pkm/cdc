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
 * @param blocks           실적에 쓸 실제 호선-블록 목록 ("2607-824,6065-518"). 비우면 가짜 번호(H1200 …)만 쓴다.
 *                         레거시 앵커와 맞는 실적을 내서 판별까지 흘려 보려고 둔다
 * @param fakeRate         blocks 가 있을 때도 가짜 번호를 낼 비율 — 앵커 없음 · 기다림 경로를 같이 시험한다
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
        @DefaultValue("true") boolean autoStart,
        @DefaultValue("") String blocks,
        @DefaultValue("0.2") double fakeRate) {

    /** blocks 를 [호선, 블록] 쌍으로. */
    public java.util.List<String[]> blockList() {
        return java.util.Arrays.stream(blocks == null ? new String[0] : blocks.split(","))
                .map(String::trim).filter(b -> b.matches("[0-9A-Z]{4}-[0-9A-Z]{3}"))
                .map(b -> b.split("-")).toList();
    }
}
