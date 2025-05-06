package rs.iggy.clients.blocking.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.ArrayUtils;
import rs.iggy.consumergroup.Consumer;
import rs.iggy.identifier.Identifier;
import rs.iggy.message.HeaderValue;
import rs.iggy.message.Message;
import rs.iggy.message.Partitioning;
import rs.iggy.message.PollingStrategy;
import rs.iggy.user.GlobalPermissions;
import rs.iggy.user.Permissions;
import rs.iggy.user.StreamPermissions;
import rs.iggy.user.TopicPermissions;
import java.math.BigInteger;
import java.util.Map;

public final class BytesSerializer {

    private BytesSerializer() {
    }

    static ByteBuf toBytes(Consumer consumer) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte(consumer.kind().asCode());
        buffer.writeBytes(toBytes(consumer.id()));
        return buffer;
    }

    static ByteBuf toBytes(Identifier identifier) {
        if (identifier.getKind() == 1) {
            ByteBuf buffer = Unpooled.buffer(6);
            buffer.writeByte(1);
            buffer.writeByte(4);
            buffer.writeIntLE(identifier.getId().intValue());
            return buffer;
        } else if (identifier.getKind() == 2) {
            ByteBuf buffer = Unpooled.buffer(2 + identifier.getName().length());
            buffer.writeByte(2);
            buffer.writeByte(identifier.getName().length());
            buffer.writeBytes(identifier.getName().getBytes());
            return buffer;
        } else {
            throw new IllegalArgumentException("Unknown identifier kind: " + identifier.getKind());
        }
    }

    static ByteBuf nameToBytes(String name) {
        ByteBuf buffer = Unpooled.buffer(1 + name.length());
        buffer.writeByte(name.length());
        buffer.writeBytes(name.getBytes());
        return buffer;
    }

    static ByteBuf toBytes(Partitioning partitioning) {
        ByteBuf buffer = Unpooled.buffer(2 + partitioning.value().length);
        buffer.writeByte(partitioning.kind().asCode());
        buffer.writeByte(partitioning.value().length);
        buffer.writeBytes(partitioning.value());
        return buffer;
    }

    static ByteBuf toBytes(Message message) {
        var buffer = Unpooled.buffer();
        buffer.writeBytes(message.id().toBytes());
        message.headers().ifPresentOrElse((headers) -> {
            var headersBytes = toBytes(headers);
            buffer.writeIntLE(headersBytes.readableBytes());
            buffer.writeBytes(headersBytes);
        }, () -> buffer.writeIntLE(0));
        buffer.writeIntLE(message.payload().length);
        buffer.writeBytes(message.payload());
        return buffer;
    }

    static ByteBuf toBytes(PollingStrategy strategy) {
        var buffer = Unpooled.buffer(9);
        buffer.writeByte(strategy.kind().asCode());
        buffer.writeBytes(toBytesAsU64(strategy.value()));
        return buffer;
    }

    static ByteBuf toBytes(Map<String, HeaderValue> headers) {
        if (headers.isEmpty()) {
            return Unpooled.EMPTY_BUFFER;
        }
        var buffer = Unpooled.buffer();
        for (Map.Entry<String, HeaderValue> entry : headers.entrySet()) {
            String key = entry.getKey();
            buffer.writeIntLE(key.length());
            buffer.writeBytes(key.getBytes());

            HeaderValue value = entry.getValue();
            buffer.writeByte(value.kind().asCode());
            buffer.writeIntLE(value.value().length());
            buffer.writeBytes(value.value().getBytes());
        }
        return buffer;
    }

    static ByteBuf toBytes(Permissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBytes(toBytes(permissions.global()));
        if (permissions.streams().isEmpty()) {
            buffer.writeByte(0);
        } else {
            for (Map.Entry<Long, StreamPermissions> entry : permissions.streams().entrySet()) {
                buffer.writeByte(1);
                buffer.writeIntLE(entry.getKey().intValue());
                buffer.writeBytes(toBytes(entry.getValue()));
            }
            buffer.writeByte(0);
        }

        return buffer;
    }

    static ByteBuf toBytes(GlobalPermissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(permissions.manageServers());
        buffer.writeBoolean(permissions.readServers());
        buffer.writeBoolean(permissions.manageUsers());
        buffer.writeBoolean(permissions.readUsers());
        buffer.writeBoolean(permissions.manageStreams());
        buffer.writeBoolean(permissions.readStreams());
        buffer.writeBoolean(permissions.manageTopics());
        buffer.writeBoolean(permissions.readTopics());
        buffer.writeBoolean(permissions.pollMessages());
        buffer.writeBoolean(permissions.sendMessages());
        return buffer;
    }

    static ByteBuf toBytes(StreamPermissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(permissions.manageStream());
        buffer.writeBoolean(permissions.readStream());
        buffer.writeBoolean(permissions.manageTopics());
        buffer.writeBoolean(permissions.readTopics());
        buffer.writeBoolean(permissions.pollMessages());
        buffer.writeBoolean(permissions.sendMessages());

        if (permissions.topics().isEmpty()) {
            buffer.writeByte(0);
        } else {
            for (Map.Entry<Long, TopicPermissions> entry : permissions.topics().entrySet()) {
                buffer.writeByte(1);
                buffer.writeIntLE(entry.getKey().intValue());
                buffer.writeBytes(toBytes(entry.getValue()));
            }
            buffer.writeByte(0);
        }

        return buffer;
    }

    static ByteBuf toBytes(TopicPermissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(permissions.manageTopic());
        buffer.writeBoolean(permissions.readTopic());
        buffer.writeBoolean(permissions.pollMessages());
        buffer.writeBoolean(permissions.sendMessages());
        return buffer;
    }

    static ByteBuf toBytesAsU64(BigInteger value) {
        if (value.signum() == -1) {
            throw new IllegalArgumentException("Negative value cannot be serialized to unsigned 64: " + value);
        }
        ByteBuf buffer = Unpooled.buffer(8, 8);
        byte[] valueAsBytes = value.toByteArray();
        if (valueAsBytes.length > 9) {
            throw new IllegalArgumentException();
        }
        ArrayUtils.reverse(valueAsBytes);
        buffer.writeBytes(valueAsBytes, 0, Math.min(8, valueAsBytes.length));
        if (valueAsBytes.length < 8) {
            buffer.writeZero(8 - valueAsBytes.length);
        }
        return buffer;
    }

    public static ByteBuf toBytesAsU128(BigInteger value) {
        if (value.signum() == -1) {
            throw new IllegalArgumentException("Negative value cannot be serialized to unsigned 128: " + value);
        }
        ByteBuf buffer = Unpooled.buffer(16, 16);
        byte[] valueAsBytes = value.toByteArray();
        if (valueAsBytes.length > 17) {
            throw new IllegalArgumentException();
        }
        ArrayUtils.reverse(valueAsBytes);
        buffer.writeBytes(valueAsBytes, 0, Math.min(16, valueAsBytes.length));
        if (valueAsBytes.length < 16) {
            buffer.writeZero(16 - valueAsBytes.length);
        }
        return buffer;
    }

}
