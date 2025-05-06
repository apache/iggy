package rs.iggy.clients.blocking.tcp;

import org.junit.jupiter.api.Test;
import rs.iggy.clients.blocking.IggyBaseClient;
import rs.iggy.clients.blocking.SystemClientBaseTest;
import rs.iggy.system.ClientInfo;
import rs.iggy.system.ClientInfoDetails;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class SystemTcpClientTest extends SystemClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

    @Test
    void shouldGetMeAndClient() {
        // when
        ClientInfoDetails me = systemClient.getMe();

        // then
        assertThat(me).isNotNull();

        // when
        var clientInfo = systemClient.getClient(me.clientId());

        // then
        assertThat(clientInfo).isNotNull();
    }

    @Test
    void shouldGetClients() {
        // when
        List<ClientInfo> clients = systemClient.getClients();

        // then
        assertThat(clients).isNotNull();
        assertThat(clients.size()).isEqualTo(1);
    }
}
