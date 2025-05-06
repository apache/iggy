package rs.iggy.clients.blocking.tcp;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.TopicsClientBaseTest;

class TopicsTcpClientTest extends TopicsClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

}
