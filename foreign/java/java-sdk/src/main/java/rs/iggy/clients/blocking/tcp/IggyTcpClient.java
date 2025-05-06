package rs.iggy.clients.blocking.tcp;

import rs.iggy.clients.blocking.*;

public class IggyTcpClient implements IggyBaseClient {

    private final UsersTcpClient usersClient;
    private final StreamsTcpClient streamsClient;
    private final TopicsTcpClient topicsClient;
    private final PartitionsTcpClient partitionsClient;
    private final ConsumerGroupsTcpClient consumerGroupsClient;
    private final ConsumerOffsetTcpClient consumerOffsetsClient;
    private final MessagesTcpClient messagesClient;
    private final SystemTcpClient systemClient;
    private final PersonalAccessTokensTcpClient personalAccessTokensClient;

    public IggyTcpClient(String host, Integer port) {
        InternalTcpClient tcpClient = new InternalTcpClient(host, port);
        tcpClient.connect();
        usersClient = new UsersTcpClient(tcpClient);
        streamsClient = new StreamsTcpClient(tcpClient);
        topicsClient = new TopicsTcpClient(tcpClient);
        partitionsClient = new PartitionsTcpClient(tcpClient);
        consumerGroupsClient = new ConsumerGroupsTcpClient(tcpClient);
        consumerOffsetsClient = new ConsumerOffsetTcpClient(tcpClient);
        messagesClient = new MessagesTcpClient(tcpClient);
        systemClient = new SystemTcpClient(tcpClient);
        personalAccessTokensClient = new PersonalAccessTokensTcpClient(tcpClient);
    }

    @Override
    public SystemClient system() {
        return systemClient;
    }

    @Override
    public StreamsClient streams() {
        return streamsClient;
    }

    @Override
    public UsersClient users() {
        return usersClient;
    }

    @Override
    public TopicsClient topics() {
        return topicsClient;
    }

    @Override
    public PartitionsClient partitions() {
        return partitionsClient;
    }

    @Override
    public ConsumerGroupsClient consumerGroups() {
        return consumerGroupsClient;
    }

    @Override
    public ConsumerOffsetsClient consumerOffsets() {
        return consumerOffsetsClient;
    }

    @Override
    public MessagesClient messages() {
        return messagesClient;
    }

    @Override
    public PersonalAccessTokensClient personalAccessTokens() {
        return personalAccessTokensClient;
    }

}
