package dev.hotdb.cdc.route;

import dev.hotdb.cdc.event.CdcEvent;
import java.util.Map;

/** 식을 평가할 때 보이는 것: 이벤트 하나와 그 이벤트에 붙인 참조 행들. */
public record EvalContext(CdcEvent event, Map<String, Map<String, Object>> lookups) {}
