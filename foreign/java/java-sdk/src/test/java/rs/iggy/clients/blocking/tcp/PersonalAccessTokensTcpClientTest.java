package rs.iggy.clients.blocking.tcp;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.PersonalAccessTokensBaseTest;

class PersonalAccessTokensTcpClientTest extends PersonalAccessTokensBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

}
