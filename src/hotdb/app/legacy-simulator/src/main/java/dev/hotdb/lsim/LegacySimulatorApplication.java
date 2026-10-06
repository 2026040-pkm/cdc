package dev.hotdb.lsim;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * 레거시 발행기. 필드 발행기(field-simulator)가 tsdb 를 채우듯, 레거시 대역(SAP · Oracle)의 **원천 표**를 계속 바꾼다 —
 * RFC Service · DB Agent 의 워터마크 폴링이 매 주기 가져갈 변경분이 생기게.
 *
 * <p>표 모양은 기동 때 JDBC 메타데이터로 읽는다 (컬럼 · PK). 로컬 검증용이라 운영에서는 띄우지 않는다.
 * 배속은 실행 중에 {@code POST /sim/speed?value=5} 로 바꾼다.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class LegacySimulatorApplication {
    public static void main(String[] args) {
        SpringApplication.run(LegacySimulatorApplication.class, args);
    }
}
