package rs.iggy.clients.blocking.http;

import rs.iggy.clients.blocking.ConsumerOffsetsClientBaseTest;
import rs.iggy.clients.blocking.IggyBaseClient;

class ConsumerOffsetsHttpClientTest extends ConsumerOffsetsClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return HttpClientFactory.create(iggyServer);
    }

}
