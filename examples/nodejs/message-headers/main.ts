import { IggyClient, Identifier, Partitioning, PollingStrategy, Message, MessageHeader, MessageHeaderValue } from '@iggy.rs/sdk';

// Define interfaces for the different message payloads
interface OrderCreated {
    orderId: number;
    currency: string;
    amount: number;
}

interface OrderConfirmed {
    orderId: number;
}

interface OrderRejected {
    orderId: number;
    reason: string;
}

// Define a type for the message type header
type MessageType = 'order_created' | 'order_confirmed' | 'order_rejected';

const IGGY_SERVER_ADDRESS = process.env.IGGY_SERVER_ADDRESS || '127.0.0.1:8090';
const STREAM_ID = Identifier.numeric(103);
const TOPIC_ID = Identifier.numeric(103);
const PARTITION_ID = 1;

// Helper function to create a message with a type header
function createTypedMessage(type: MessageType, payload: object): Message {
    const headers = new Map<string, MessageHeaderValue>();
    headers.set('type', MessageHeader.fromString(type));
    return new Message(Buffer.from(JSON.stringify(payload)), undefined, headers);
}

async function runExample() {
    console.log('Message Headers example started...');

    const client = IggyClient.create(IGGY_SERVER_ADDRESS, { protocol: 'tcp' });
    await client.connect();
    await client.loginUser({ username: 'root', password: 'password' });
    console.log('Connected and logged in to Iggy server.');

    try {
        await client.createStream({ name: 'headers-stream', streamId: STREAM_ID.value });
        console.log(`Stream with ID '${STREAM_ID.value}' created.`);
        await client.createTopic({ streamId: STREAM_ID, topicId: TOPIC_ID.value, partitionsCount: 1, name: 'headers-topic' });
        console.log(`Topic with ID '${TOPIC_ID.value}' created.`);

        console.log('Sending messages with headers...');
        const messages: Message[] = [
            createTypedMessage('order_created', { orderId: 1, currency: 'USD', amount: 100.0 }),
            createTypedMessage('order_confirmed', { orderId: 1 }),
            createTypedMessage('order_rejected', { orderId: 2, reason: 'out of stock' }),
            createTypedMessage('order_created', { orderId: 3, currency: 'EUR', amount: 250.5 }),
            createTypedMessage('order_confirmed', { orderId: 3 })
        ];

        await client.sendMessages({
            streamId: STREAM_ID,
            topicId: TOPIC_ID,
            partitioning: Partitioning.partitionId(PARTITION_ID),
            messages
        });
        console.log('Messages sent successfully.');

        const polledMessages = await client.pollMessages({
            streamId: STREAM_ID,
            topicId: TOPIC_ID,
            partitionId: PARTITION_ID,
            pollingStrategy: PollingStrategy.offset(0),
            count: messages.length,
            autoCommit: false
        });

        console.log(`Polled ${polledMessages.messages.length} messages:`);
        for (const msg of polledMessages.messages) {
            const messageType = msg.headers?.get('type')?.asString() as MessageType;
            switch (messageType) {
                case 'order_created':
                    const created = JSON.parse(msg.payload.toString()) as OrderCreated;
                    console.log(`  - [OrderCreated] ID: ${created.orderId}, Amount: ${created.amount} ${created.currency}`);
                    break;
                case 'order_confirmed':
                    const confirmed = JSON.parse(msg.payload.toString()) as OrderConfirmed;
                    console.log(`  - [OrderConfirmed] ID: ${confirmed.orderId}`);
                    break;
                case 'order_rejected':
                    const rejected = JSON.parse(msg.payload.toString()) as OrderRejected;
                    console.log(`  - [OrderRejected] ID: ${rejected.orderId}, Reason: ${rejected.reason}`);
                    break;
                default:
                    console.log(`  - [Unknown] Received message with unknown type: ${messageType}`);
            }
        }

    } catch (e) {
        console.error('An error occurred:', e);
    } finally {
        await client.deleteStream({ streamId: STREAM_ID });
        console.log(`Stream with ID '${STREAM_ID.value}' deleted.`);
        await client.disconnect();
        console.log('Disconnected from Iggy server.');
    }
}

runExample().catch(e => {
    console.error('\nAn error occurred:', e);
    process.exit(1);
});
