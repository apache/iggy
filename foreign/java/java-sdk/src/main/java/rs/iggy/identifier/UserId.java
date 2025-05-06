package rs.iggy.identifier;

import javax.annotation.Nullable;

public class UserId extends Identifier {

    private UserId(@Nullable String name, @Nullable Long id) {
        super(name, id);
    }

    public static UserId of(String name) {
        return new UserId(name, null);
    }

    public static UserId of(Long id) {
        return new UserId(null, id);
    }

}
