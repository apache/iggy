package rs.iggy.clients.blocking;

import rs.iggy.system.ClientInfo;
import rs.iggy.system.ClientInfoDetails;
import rs.iggy.system.Stats;
import java.util.List;

public interface SystemClient {

    Stats getStats();

    ClientInfoDetails getMe();

    ClientInfoDetails getClient(Long clientId);

    List<ClientInfo> getClients();

    String ping();

}
