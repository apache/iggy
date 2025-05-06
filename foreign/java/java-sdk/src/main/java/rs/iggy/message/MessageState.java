package rs.iggy.message;

public enum MessageState {
    Available(1),
    Unavailable(10),
    Poisoned(20),
    MarkedForDeletion(30);

    private final int code;

    MessageState(int code) {
        this.code = code;
    }

    public static MessageState fromCode(int code) {
        for (MessageState state : MessageState.values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown message state code: " + code);
    }
}
