package rs.iggy.message;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum PartitioningKind {
    @JsonProperty("balanced")
    Balanced(1),
    @JsonProperty("partition_id")
    PartitionId(2),
    @JsonProperty("messages_key")
    MessagesKey(3);

    private final int code;

    PartitioningKind(int code) {
        this.code = code;
    }

    public int asCode() {
        return code;
    }
}
