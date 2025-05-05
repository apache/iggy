package rs.iggy.clients.blocking.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

final class InternalTcpClient {

    private static final int REQUEST_INITIAL_BYTES_LENGTH = 4;
    private static final int COMMAND_LENGTH = 4;
    private static final int RESPONSE_INITIAL_BYTES_LENGTH = 8;

    private final TcpClient client;
    private final BlockingQueue<IggyResponse> responses = new LinkedBlockingQueue<>();
    private Connection connection;

    InternalTcpClient(String host, Integer port) {
        client = TcpClient.create()
                .host(host)
                .port(port)
                .doOnConnected(conn -> conn.addHandlerLast(new IggyResponseDecoder()));
    }

    void connect() {
        this.connection = client.connectNow();
        this.connection.inbound().receiveObject().ofType(IggyResponse.class).subscribe(responses::add);
    }

    ByteBuf send(int command) {
        return send(command, Unpooled.EMPTY_BUFFER);
    }

    ByteBuf send(int command, ByteBuf payload) {
        var payloadSize = payload.readableBytes() + COMMAND_LENGTH;
        var buffer = Unpooled.buffer(REQUEST_INITIAL_BYTES_LENGTH + payloadSize);
        buffer.writeIntLE(payloadSize);
        buffer.writeIntLE(command);
        buffer.writeBytes(payload);

        connection.outbound().send(Mono.just(buffer)).then().block();
        try {
            IggyResponse response = responses.take();
            return handleResponse(response);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ByteBuf handleResponse(IggyResponse response) {
        if (response.status() != 0) {
            throw new RuntimeException("Received an invalid response with status " + response.status());
        }
        if (response.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        return response.payload();
    }

    static class IggyResponseDecoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) {
            if (byteBuf.readableBytes() < RESPONSE_INITIAL_BYTES_LENGTH) {
                return;
            }
            byteBuf.markReaderIndex();
            var status = byteBuf.readUnsignedIntLE();
            var responseLength = byteBuf.readUnsignedIntLE();
            if (byteBuf.readableBytes() < responseLength) {
                byteBuf.resetReaderIndex();
                return;
            }
            var length = Long.valueOf(responseLength).intValue();
            list.add(new IggyResponse(status, length, byteBuf.readBytes(length)));
        }
    }

    record IggyResponse(long status, int length, ByteBuf payload) {
    }

}
