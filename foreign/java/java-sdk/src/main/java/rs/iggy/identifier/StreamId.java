package rs.iggy.identifier;

import javax.annotation.Nullable;

public class StreamId extends Identifier {

    private StreamId(@Nullable String name, @Nullable Long id) {
        super(name, id);
    }

    public static StreamId of(String name) {
        return new StreamId(name, null);
    }

    public static StreamId of(Long id) {
        return new StreamId(null, id);
    }

}
