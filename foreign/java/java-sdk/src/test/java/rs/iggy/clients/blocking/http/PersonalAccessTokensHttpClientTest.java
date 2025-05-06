package rs.iggy.clients.blocking.http;

import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.PersonalAccessTokensBaseTest;

class PersonalAccessTokensHttpClientTest extends PersonalAccessTokensBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return HttpClientFactory.create(iggyServer);
    }

}
