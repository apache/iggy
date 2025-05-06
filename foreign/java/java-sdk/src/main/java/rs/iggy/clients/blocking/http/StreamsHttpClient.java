package rs.iggy.clients.blocking.http;

import com.fasterxml.jackson.core.type.TypeReference;
import rs.iggy.clients.blocking.StreamsClient;
import rs.iggy.identifier.StreamId;
import rs.iggy.stream.StreamBase;
import rs.iggy.stream.StreamDetails;
import java.util.List;
import java.util.Optional;

class StreamsHttpClient implements StreamsClient {

    private static final String STREAMS = "/streams";
    private final InternalHttpClient httpClient;

    public StreamsHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Optional<StreamDetails> getStream(StreamId streamId) {
        var request = httpClient.prepareGetRequest(STREAMS + "/" + streamId);
        return httpClient.executeWithOptionalResponse(request, StreamDetails.class);
    }

    @Override
    public List<StreamBase> getStreams() {
        var request = httpClient.prepareGetRequest(STREAMS);
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public StreamDetails createStream(Optional<Long> streamId, String name) {
        var request = httpClient.preparePostRequest(STREAMS, new CreateStream(streamId, name));
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public void updateStream(StreamId streamId, String name) {
        var request = httpClient.preparePutRequest(STREAMS + "/" + streamId, new UpdateStream(name));
        httpClient.execute(request);
    }

    @Override
    public void deleteStream(StreamId streamId) {
        var request = httpClient.prepareDeleteRequest(STREAMS + "/" + streamId);
        httpClient.execute(request);
    }

    record CreateStream(Optional<Long> streamId, String name) {
    }

    record UpdateStream(String name) {
    }
}
