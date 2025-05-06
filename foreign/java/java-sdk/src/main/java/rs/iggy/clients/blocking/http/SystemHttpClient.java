package rs.iggy.clients.blocking.http;

import com.fasterxml.jackson.core.type.TypeReference;
import rs.iggy.clients.blocking.SystemClient;
import rs.iggy.system.ClientInfo;
import rs.iggy.system.ClientInfoDetails;
import rs.iggy.system.Stats;
import java.util.List;

class SystemHttpClient implements SystemClient {

    private static final String STATS = "/stats";
    private static final String CLIENTS = "/clients";
    private static final String PING = "/ping";
    private final InternalHttpClient httpClient;

    public SystemHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Stats getStats() {
        var request = httpClient.prepareGetRequest(STATS);
        return httpClient.execute(request, Stats.class);
    }

    @Override
    public ClientInfoDetails getMe() {
        throw new UnsupportedOperationException("Method not available in HTTP client");
    }

    @Override
    public ClientInfoDetails getClient(Long clientId) {
        var request = httpClient.prepareGetRequest(CLIENTS + "/" + clientId);
        return httpClient.execute(request, ClientInfoDetails.class);
    }

    @Override
    public List<ClientInfo> getClients() {
        var request = httpClient.prepareGetRequest(CLIENTS);
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public String ping() {
        var request = httpClient.prepareGetRequest(PING);
        return httpClient.executeWithStringResponse(request);
    }
}
