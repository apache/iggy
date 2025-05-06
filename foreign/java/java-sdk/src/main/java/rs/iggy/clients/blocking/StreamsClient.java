package rs.iggy.clients.blocking;

import rs.iggy.identifier.StreamId;
import rs.iggy.stream.StreamBase;
import rs.iggy.stream.StreamDetails;
import java.util.List;
import java.util.Optional;

public interface StreamsClient {

    default Optional<StreamDetails> getStream(Long streamId) {
        return getStream(StreamId.of(streamId));
    }

    Optional<StreamDetails> getStream(StreamId streamId);

    List<StreamBase> getStreams();

    StreamDetails createStream(Optional<Long> streamId, String name);

    default void updateStream(Long streamId, String name) {
        updateStream(StreamId.of(streamId), name);
    }

    void updateStream(StreamId streamId, String name);

    default void deleteStream(Long streamId) {
        deleteStream(StreamId.of(streamId));
    }

    void deleteStream(StreamId streamId);

}
