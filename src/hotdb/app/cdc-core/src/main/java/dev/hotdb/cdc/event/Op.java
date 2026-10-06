package dev.hotdb.cdc.event;

/** Debezium op 코드. r 은 스냅샷 읽기 — no_data 스냅샷이면 오지 않는다. */
public enum Op {
    CREATE("c"), UPDATE("u"), DELETE("d"), READ("r"), TRUNCATE("t");

    private final String code;

    Op(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    public static Op of(String code) {
        for (Op op : values()) {
            if (op.code.equals(code)) {
                return op;
            }
        }
        throw new IllegalArgumentException("모르는 op: " + code);
    }
}
