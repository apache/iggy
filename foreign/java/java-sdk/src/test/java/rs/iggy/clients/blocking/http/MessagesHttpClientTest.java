package rs.iggy.clients.blocking.http;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.MessagesClientBaseTest;

class MessagesHttpClientTest extends MessagesClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return HttpClientFactory.create(iggyServer);
    }

}
