package rs.iggy.clients.blocking;

public class IggyClientBuilder {

    private IggyBaseClient client;

    public IggyClientBuilder withBaseClient(IggyBaseClient client) {
        this.client = client;
        return this;
    }

    public IggyClient build() {
        if (client == null) {
            throw new IllegalArgumentException("Base client not provided");
        }
        return new IggyClient(client);
    }

}
