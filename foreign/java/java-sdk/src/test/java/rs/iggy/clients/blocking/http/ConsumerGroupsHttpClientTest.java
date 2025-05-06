package rs.iggy.clients.blocking.http;

import rs.iggy.clients.blocking.ConsumerGroupsClientBaseTest;
import rs.iggy.clients.blocking.IggyBaseClient;

class ConsumerGroupsHttpClientTest extends ConsumerGroupsClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return HttpClientFactory.create(iggyServer);
    }

}
