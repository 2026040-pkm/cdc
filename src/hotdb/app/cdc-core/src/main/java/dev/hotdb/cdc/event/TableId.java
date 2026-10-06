package dev.hotdb.cdc.event;

/** 스키마 한정 테이블 이름. 청크 이름이 아니라 논리 이름(하이퍼테이블 부모)이어야 라우트와 맞는다. */
public record TableId(String schema, String table) {

    public static TableId parse(String qualified) {
        int dot = qualified.indexOf('.');
        if (dot <= 0 || dot == qualified.length() - 1) {
            throw new IllegalArgumentException("schema.table 형식이어야 합니다: " + qualified);
        }
        return new TableId(qualified.substring(0, dot), qualified.substring(dot + 1));
    }

    @Override
    public String toString() {
        return schema + "." + table;
    }
}
