import { IggyClient, Identifier, Partitioning, PollingStrategy, Message } from '@iggy.rs/sdk';

// Define interfaces for the different message types and the envelope
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

interface Envelope {
    type: 'order_created' | 'order_confirmed' | 'order_rejected';
    payload: OrderCreated | OrderConfirmed | OrderRejected;
}

const IGGY_SERVER_ADDRESS = process.env.IGGY_SERVER_ADDRESS || '127.0.0.1:8090';
const STREAM_ID = Identifier.numeric(102);
const TOPIC_ID = Identifier.numeric(102);
const PARTITION_ID = 1;

// Helper function to create a message with an envelope
function createEnvelopeMessage(envelope: Envelope): Message {
    return new Message(Buffer.from(JSON.stringify(envelope)));
}

async function runExample() {
    console.log('Message Envelope example started...');

    const client = IggyClient.create(IGGY_SERVER_ADDRESS, { protocol: 'tcp' });
    await client.connect();
    await client.loginUser({ username: 'root', password: 'password' });
    console.log('Connected and logged in to Iggy server.');

    try {
        await client.createStream({ name: 'envelope-stream', streamId: STREAM_ID.value });
        console.log(`Stream with ID '${STREAM_ID.value}' created.`);
        await client.createTopic({ streamId: STREAM_ID, topicId: TOPIC_ID.value, partitionsCount: 1, name: 'envelope-topic' });
        console.log(`Topic with ID '${TOPIC_ID.value}' created.`);

        console.log('Sending messages with envelopes...');
        const messages: Message[] = [
            createEnvelopeMessage({ type: 'order_created', payload: { orderId: 1, currency: 'USD', amount: 100.0 } }),
            createEnvelopeMessage({ type: 'order_confirmed', payload: { orderId: 1 } }),
            createEnvelopeMessage({ type: 'order_rejected', payload: { orderId: 2, reason: 'out of stock' } }),
            createEnvelopeMessage({ type: 'order_created', payload: { orderId: 3, currency: 'EUR', amount: 250.5 } }),
            createEnvelopeMessage({ type: 'order_confirmed', payload: { orderId: 3 } })
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
            const envelope: Envelope = JSON.parse(msg.payload.toString());
            switch (envelope.type) {
                case 'order_created':
                    const created = envelope.payload as OrderCreated;
                    console.log(`  - [OrderCreated] ID: ${created.orderId}, Amount: ${created.amount} ${created.currency}`);
                    break;
                case 'order_confirmed':
                    const confirmed = envelope.payload as OrderConfirmed;
                    console.log(`  - [OrderConfirmed] ID: ${confirmed.orderId}`);
                    break;
                case 'order_rejected':
                    const rejected = envelope.payload as OrderRejected;
                    console.log(`  - [OrderRejected] ID: ${rejected.orderId}, Reason: ${rejected.reason}`);
                    break;
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
