package rs.iggy.message;

public enum HeaderKind {
    Raw(1),
    String(2),
    Bool(3),
    Int8(4),
    Int16(5),
    Int32(6),
    Int64(7),
    Int128(8),
    Uint8(9),
    Uint16(10),
    Uint32(11),
    Uint64(12),
    Uint128(13),
    Float32(14),
    Float64(15);

    private final int code;

    HeaderKind(int code) {
        this.code = code;
    }

    public static HeaderKind fromCode(int code) {
        for (HeaderKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        throw new IllegalArgumentException("Unknown header kind: " + code);
    }

    public int asCode() {
        return code;
    }
}
