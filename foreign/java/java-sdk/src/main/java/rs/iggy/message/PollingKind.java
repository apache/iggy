package rs.iggy.message;

public enum PollingKind {
    Offset(1),
    Timestamp(2),
    First(3),
    Last(4),
    Next(5);

    private final int code;

    PollingKind(int code) {
        this.code = code;
    }

    public int asCode() {
        return code;
    }
}
