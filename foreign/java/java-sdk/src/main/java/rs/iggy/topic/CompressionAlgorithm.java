package rs.iggy.topic;

public enum CompressionAlgorithm {
    None(1),
    Gzip(2);

    private final Integer code;

    CompressionAlgorithm(Integer code) {
        this.code = code;
    }

    public static CompressionAlgorithm fromCode(byte code) {
        for (CompressionAlgorithm algorithm : values()) {
            if (algorithm.code == code) {
                return algorithm;
            }
        }
        throw new IllegalArgumentException();
    }

    public Integer asCode() {
        return code;
    }
}
