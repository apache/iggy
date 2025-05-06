package rs.iggy.clients.blocking.http;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.StreamClientBaseTest;

class StreamHttpClientTest extends StreamClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return HttpClientFactory.create(iggyServer);
    }

}
