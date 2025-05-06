package rs.iggy.clients.blocking.http;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.PartitionsClientBaseTest;

class PartitionsHttpClientTest extends PartitionsClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return HttpClientFactory.create(iggyServer);
    }

}
