package rs.iggy.clients.blocking.tcp;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.MessagesClientBaseTest;

class MessagesTcpClientTest extends MessagesClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

}
