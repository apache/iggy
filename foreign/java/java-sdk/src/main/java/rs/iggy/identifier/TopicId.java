package rs.iggy.identifier;

import javax.annotation.Nullable;

public class TopicId extends Identifier {

    private TopicId(@Nullable String name, @Nullable Long id) {
        super(name, id);
    }

    public static TopicId of(String name) {
        return new TopicId(name, null);
    }

    public static TopicId of(Long id) {
        return new TopicId(null, id);
    }

}
