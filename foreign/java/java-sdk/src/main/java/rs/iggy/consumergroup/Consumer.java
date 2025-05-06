package rs.iggy.consumergroup;

import rs.iggy.identifier.ConsumerId;

public record Consumer(Kind kind, ConsumerId id) {

    public static Consumer of(Long id) {
        return new Consumer(Kind.Consumer, ConsumerId.of(id));
    }

    public static Consumer of(ConsumerId id) {
        return new Consumer(Kind.Consumer, id);
    }

    public static Consumer group(Long id) {
        return new Consumer(Kind.ConsumerGroup, ConsumerId.of(id));
    }

    public static Consumer group(ConsumerId id) {
        return new Consumer(Kind.ConsumerGroup, id);
    }

    public enum Kind {
        Consumer(1),
        ConsumerGroup(2);

        private final int code;

        Kind(int code) {
            this.code = code;
        }

        public int asCode() {
            return code;
        }

    }
}
