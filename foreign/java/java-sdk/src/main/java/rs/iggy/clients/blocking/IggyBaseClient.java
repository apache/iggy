package rs.iggy.clients.blocking;

public interface IggyBaseClient {

    SystemClient system();

    StreamsClient streams();

    UsersClient users();

    TopicsClient topics();

    PartitionsClient partitions();

    ConsumerGroupsClient consumerGroups();

    ConsumerOffsetsClient consumerOffsets();

    MessagesClient messages();

    PersonalAccessTokensClient personalAccessTokens();

}
