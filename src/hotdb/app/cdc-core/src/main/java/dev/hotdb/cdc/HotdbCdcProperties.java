package dev.hotdb.cdc;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * CDC 접속 · 엔진 설정 ({@code hotdb.cdc.*}).
 *
 * <p>한 프로세스 = 파이프라인 하나 = 복제 슬롯 하나. 같은 DB 에 소비자를 여럿 붙일 때
 * {@code pipeline · slot · offset-file} 세 값이 인스턴스마다 달라야 한다 — 겹치면 슬롯을 다투거나
 * 오프셋을 서로 덮어쓴다.
 *
 * @param adapter       {@link dev.hotdb.cdc.port.CdcSource} 구현을 고른다. 지금은 debezium 하나
 * @param includeTables Debezium table.include.list 정규식. 청크는 {@code _timescaledb_internal\._hyper_[0-9]+_[0-9]+_chunk}
 *                      로 받는다. Debezium 은 정규식을 전체 일치로 보므로 압축 뭉치(*_compressed)는 걸리지 않는다
 * @param snapshotMode  no_data — 이력은 수십억 행이라 초기 스냅샷을 하지 않는다 (BRD §05.3 B6)
 * @param strictSource  라우트가 참조하는 원천 컬럼이 없으면 기동을 거부한다. false 면 경고만
 * @param exitOnFailure 엔진이 죽으면 프로세스를 끝낸다 — 컨테이너 재시작에 맡긴다. 스스로 멈춘 경우(정지 판정 ·
 *                      캡처 갭)는 끝내지 않는다 — 재시작해도 같은 이유로 멈추므로, 살아서 health DOWN 과 지표를 보인다
 * @param failOnCaptureGap 기동 때 되받을 수 없는 구간을 찾으면 엔진을 띄우지 않는다 (조용히 어긋난 채 도는 것보다 멈춘다)
 * @param maxQueueSize  엔진 내부 큐 건수 상한. 꽉 차면 슬롯 읽기가 멈춘다(backpressure) — 밀린 것은 프로세스가 아니라
 *                      원천 슬롯(WAL)에 쌓인다. 적체 때 프로세스 메모리 상한이 여기서 정해진다
 * @param maxQueueSizeBytes 같은 큐의 바이트 상한 (Debezium max.queue.size.in.bytes). 0 = 건수로만 막는다.
 *                      행이 큰 표(산출물 행렬 등)가 섞이면 건수만으로는 메모리가 안 묶이므로 이것을 건다
 * @param deadLetter    격리된 이벤트 재처리
 * @param dbWait        기동 때 Hot DB 가 응답할 때까지 기다리는 간격 · 상한 ({@link DbReadyGate})
 * @param engineEnabled false 면 엔진만 빼고 같은 프로세스를 띄운다 — 엔진 자원 측정의 기준선(scripts/engine-resource.py).
 *                      슬롯을 읽지 않으므로 운영에서는 끄지 않는다
 * @param debezium      그 밖의 Debezium 속성을 그대로 넘긴다 (코드 수정 없이 튜닝)
 */
@ConfigurationProperties("hotdb.cdc")
public record HotdbCdcProperties(
        String pipeline,
        @DefaultValue("debezium") String adapter,
        Source source,
        String slot,
        String publication,
        List<String> includeTables,
        @DefaultValue("no_data") String snapshotMode,
        String offsetFile,
        @DefaultValue("10000") long heartbeatIntervalMs,
        @DefaultValue("2048") int maxBatchSize,
        @DefaultValue("8192") int maxQueueSize,
        @DefaultValue("0") long maxQueueSizeBytes,
        @DefaultValue("200") long pollIntervalMs,
        @DefaultValue Apply apply,
        @DefaultValue("true") boolean strictSource,
        @DefaultValue("true") boolean exitOnFailure,
        @DefaultValue("true") boolean failOnCaptureGap,
        @DefaultValue DeadLetter deadLetter,
        @DefaultValue DbWait dbWait,
        @DefaultValue("true") boolean engineEnabled,
        Map<String, String> debezium) {

    public record Source(String host, @DefaultValue("5432") int port, String database, String user, String password) {}

    /**
     * @param maxRetries      배치 전체 재시도 횟수. 소진하면 건 단위로 좁혀 실패한 건만 dead letter 로 보낸다
     * @param backoffMs       첫 재시도 대기 (시도마다 두 배)
     * @param haltRatio       건 단위로 좁혔을 때 이 비율을 넘게 실패하면 개별 데이터가 아니라 구조 문제로 보고 멈춘다 —
     *                        그러지 않으면 대상 표가 사라졌을 때 dead letter 가 전체 트래픽을 삼킨다
     * @param haltMinFailures 비율 판정을 하는 최소 실패 건수. 한두 건짜리 배치에서 독이 든 행 하나로 멈추지 않게
     */
    public record Apply(@DefaultValue("3") int maxRetries, @DefaultValue("200") long backoffMs,
                        @DefaultValue("0.5") double haltRatio, @DefaultValue("10") int haltMinFailures) {}

    /**
     * PENDING 은 자동으로 집지 않는다 — 원인이 고쳐졌는지는 사람만 안다. 고친 뒤
     * {@code UPDATE ops.cdc_dead_letter SET status = 'RETRY_REQUESTED' WHERE id = ...} 로 표시한 건만 다시 반영한다.
     */
    public record DeadLetter(@DefaultValue("true") boolean reprocessEnabled,
                             @DefaultValue("30000") long reprocessIntervalMs,
                             @DefaultValue("50") int reprocessBatchSize) {}

    /** @param intervalMs 시도 간격 · @param timeoutMs 이 시간 안에 응답이 없으면 기동 실패 (0 = 무한히 기다린다) */
    public record DbWait(@DefaultValue("5000") long intervalMs, @DefaultValue("0") long timeoutMs) {}

    @Override
    public Map<String, String> debezium() {
        return debezium == null ? Map.of() : debezium;
    }
}
