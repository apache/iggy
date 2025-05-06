package rs.iggy.identifier;

import javax.annotation.Nullable;

public class ConsumerId extends Identifier {

    private ConsumerId(@Nullable String name, @Nullable Long id) {
        super(name, id);
    }

    public static ConsumerId of(String name) {
        return new ConsumerId(name, null);
    }

    public static ConsumerId of(Long id) {
        return new ConsumerId(null, id);
    }

}
