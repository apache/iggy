package rs.iggy.identifier;

import org.apache.commons.lang3.StringUtils;
import javax.annotation.Nullable;

public abstract class Identifier {

    private final String name;
    private final Long id;

    protected Identifier(@Nullable String name, @Nullable Long id) {
        if (StringUtils.isBlank(name) && id == null) {
            throw new IllegalArgumentException("Name and id cannot be blank");
        }
        if (StringUtils.isNotBlank(name) && id != null) {
            throw new IllegalArgumentException("Name and id cannot be both present");
        }
        if (StringUtils.isNotBlank(name)) {
            this.name = name;
            this.id = null;
        } else {
            this.name = null;
            this.id = id;
        }
    }

    @Override
    public String toString() {
        if (StringUtils.isNotBlank(name)) {
            return name;
        }
        return id.toString();
    }

    public int getKind() {
        if (id != null) {
            return 1;
        }
        return 2;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

}

