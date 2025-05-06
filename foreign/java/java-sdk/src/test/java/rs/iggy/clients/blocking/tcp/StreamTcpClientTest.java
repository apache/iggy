package rs.iggy.clients.blocking.tcp;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.StreamClientBaseTest;

class StreamTcpClientTest extends StreamClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

}
