package rs.iggy.clients.blocking;

public class IggyClient {

    private final IggyBaseClient baseClient;

    public IggyClient(IggyBaseClient baseClient) {
        this.baseClient = baseClient;
    }

    public IggyBaseClient getBaseClient() {
        return baseClient;
    }

}
