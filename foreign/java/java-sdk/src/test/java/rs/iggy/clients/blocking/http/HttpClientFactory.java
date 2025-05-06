package rs.iggy.clients.blocking.http;

import org.testcontainers.containers.GenericContainer;
import static rs.iggy.clients.blocking.IntegrationTest.HTTP_PORT;

class HttpClientFactory {

    static IggyHttpClient create(GenericContainer<?> iggyServer) {
        String address = iggyServer.getHost();
        Integer port = iggyServer.getMappedPort(HTTP_PORT);
        return new IggyHttpClient("http://" + address + ":" + port);
    }

}
