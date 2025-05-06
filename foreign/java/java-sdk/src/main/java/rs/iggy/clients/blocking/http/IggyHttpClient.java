package rs.iggy.clients.blocking.http;

import rs.iggy.clients.blocking.*;

public class IggyHttpClient implements IggyBaseClient {

    private final SystemHttpClient systemClient;
    private final StreamsHttpClient streamsClient;
    private final UsersHttpClient usersClient;
    private final TopicsHttpClient topicsClient;
    private final PartitionsHttpClient partitionsClient;
    private final ConsumerGroupsHttpClient consumerGroupsClient;
    private final ConsumerOffsetsHttpClient consumerOffsetsClient;
    private final MessagesHttpClient messagesClient;
    private final PersonalAccessTokensHttpClient personalAccessTokensHttpClient;

    public IggyHttpClient(String url) {
        InternalHttpClient httpClient = new InternalHttpClient(url);
        systemClient = new SystemHttpClient(httpClient);
        streamsClient = new StreamsHttpClient(httpClient);
        usersClient = new UsersHttpClient(httpClient);
        topicsClient = new TopicsHttpClient(httpClient);
        partitionsClient = new PartitionsHttpClient(httpClient);
        consumerGroupsClient = new ConsumerGroupsHttpClient(httpClient);
        consumerOffsetsClient = new ConsumerOffsetsHttpClient(httpClient);
        messagesClient = new MessagesHttpClient(httpClient);
        personalAccessTokensHttpClient = new PersonalAccessTokensHttpClient(httpClient);
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
        return personalAccessTokensHttpClient;
    }

}
