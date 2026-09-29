package dev.embeddedcdc.application.handler;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.RowData;
import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.out.PassthroughRepository;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * source 와 target 컬럼이 같은 테이블용 핸들러. 컬럼 목록만 다르고 동작은 같다.
 *
 * car 와 같은 규칙으로 움직인다 — upsert 와 delete 모두 저장된 LSN 보다 새로울 때만 반영하고,
 * 삭제는 하드 삭제다(소프트 삭제는 computer 처럼 "삭제 뒤 늦게 온 UPDATE 가 행을 되살리는" 경우를
 * 보여 주기 위한 것이고, 여기서는 그 시연이 목적이 아니다).
 *
 * 값은 이벤트에 실린 문자열 그대로 넘긴다. 키 컬럼이 이벤트에 없으면 예외다 — 조용히 null 로
 * 넣으면 잘못된 행이 적재되고 한참 뒤에 발견된다(RowData 의 원칙과 같다). 나머지 컬럼은
 * null 을 그대로 허용한다(lidar_device_state.error_code 처럼 실제로 NULL 인 값이 있다).
 */
@Slf4j
public class PassthroughSyncHandler implements TableSyncHandler {

    private final SourceTable table;
    private final List<String> keyColumns;
    private final List<String> columns;
    private final PassthroughRepository repository;

    public PassthroughSyncHandler(SourceTable table, List<String> keyColumns, List<String> columns,
                                  PassthroughRepository repository) {
        if (!columns.containsAll(keyColumns)) {
            throw new IllegalArgumentException(table + ": 키 컬럼이 컬럼 목록에 없다 " + keyColumns);
        }
        this.table = table;
        this.keyColumns = List.copyOf(keyColumns);
        this.columns = List.copyOf(columns);
        this.repository = repository;
    }

    @Override
    public SourceTable table() {
        return table;
    }

    @Override
    public int apply(ChangeEvent event) {
        int affected;
        if (event.op().isUpsert()) {
            Map<String, String> values = pick(event.after(), columns);
            affected = repository.upsertIfNewer(table.tableName(), keyColumns, columns, values, event.lsn());
        } else {
            Map<String, String> keys = pick(event.before(), keyColumns);
            affected = repository.deleteIfNewer(table.tableName(), keyColumns, keys, event.lsn());
        }
        if (affected == 0) {
            log.debug("더 오래된 이벤트라 차단됨 table={} op={} lsn={}", table.tableName(), event.op().code(), event.lsn());
        }
        return affected;
    }

    private Map<String, String> pick(RowData row, List<String> wanted) {
        Map<String, String> out = new HashMap<>();
        for (String column : wanted) {
            if (keyColumns.contains(column)) {
                out.put(column, row.text(column));   // 키는 반드시 있어야 한다
            } else {
                out.put(column, row.values().get(column));
            }
        }
        return out;
    }
}
