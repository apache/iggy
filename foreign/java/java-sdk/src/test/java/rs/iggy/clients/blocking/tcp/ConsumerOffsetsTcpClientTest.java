package rs.iggy.clients.blocking.tcp;

import rs.iggy.clients.blocking.ConsumerOffsetsClientBaseTest;
import rs.iggy.clients.blocking.IggyBaseClient;

class ConsumerOffsetsTcpClientTest extends ConsumerOffsetsClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

}
