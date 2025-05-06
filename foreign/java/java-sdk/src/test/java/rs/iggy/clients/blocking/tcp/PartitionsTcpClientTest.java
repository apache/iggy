package rs.iggy.clients.blocking.tcp;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.PartitionsClientBaseTest;

class PartitionsTcpClientTest extends PartitionsClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

}
