package rs.iggy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rs.iggy.clients.blocking.tcp.IggyTcpClient;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import rs.iggy.message.Message;
import rs.iggy.message.Partitioning;
import rs.iggy.stream.StreamDetails;
import rs.iggy.topic.CompressionAlgorithm;
import rs.iggy.topic.TopicDetails;
import java.math.BigInteger;
import java.util.Optional;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;

public class SimpleProducer {
    private static final String STREAM_NAME = "dev01";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);
    private static final String TOPIC_NAME = "events";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);
    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String[] args) {
        var client = new IggyTcpClient("localhost", 8090);
        client.users().login("iggy", "iggy");

        createStream(client);
        createTopic(client);

        int counter = 0;
        while (counter++ < 1000) {
            var message = Message.of("message from simple producer " + counter);
            client.messages().sendMessages(STREAM_ID, TOPIC_ID, Partitioning.balanced(), singletonList(message));
            log.debug("Message {} sent", counter);
        }

    }

    private static void createStream(IggyTcpClient client) {
        Optional<StreamDetails> stream = client.streams().getStream(STREAM_ID);
        if (stream.isPresent()) {
            return;
        }
        client.streams().createStream(empty(), STREAM_NAME);
    }

    private static void createTopic(IggyTcpClient client) {
        Optional<TopicDetails> topic = client.topics().getTopic(STREAM_ID, TOPIC_ID);
        if (topic.isPresent()) {
            return;
        }
        client.topics()
                .createTopic(STREAM_ID,
                        empty(),
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        empty(),
                        TOPIC_NAME);
    }

}
