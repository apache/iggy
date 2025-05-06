package rs.iggy.clients.blocking.http;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.SystemClientBaseTest;

class SystemHttpClientTest extends SystemClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return HttpClientFactory.create(iggyServer);
    }

}
