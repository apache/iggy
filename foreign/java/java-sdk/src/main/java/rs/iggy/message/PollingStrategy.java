package rs.iggy.message;

import java.math.BigInteger;

public record PollingStrategy(PollingKind kind, BigInteger value) {

    public static PollingStrategy offset(BigInteger value) {
        return new PollingStrategy(PollingKind.Offset, value);
    }

    public static PollingStrategy timestamp(BigInteger value) {
        return new PollingStrategy(PollingKind.Timestamp, value);
    }

    public static PollingStrategy first() {
        return new PollingStrategy(PollingKind.First, BigInteger.ZERO);
    }

    public static PollingStrategy last() {
        return new PollingStrategy(PollingKind.Last, BigInteger.ZERO);
    }

    public static PollingStrategy next() {
        return new PollingStrategy(PollingKind.Next, BigInteger.ZERO);
    }

}
