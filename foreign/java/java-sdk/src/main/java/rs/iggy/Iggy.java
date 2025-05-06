package rs.iggy;

import rs.iggy.clients.blocking.IggyClientBuilder;

public final class Iggy {

    private Iggy() {
    }

    public static IggyClientBuilder clientBuilder() {
        return new IggyClientBuilder();
    }

}
