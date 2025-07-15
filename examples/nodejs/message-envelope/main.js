import { IggyClient, Identifier, Partitioning, PollingStrategy, Message, CompressionAlgorithm } from '@iggy.rs/sdk';

const STREAM_ID = Identifier.numeric(3);
const TOPIC_ID = Identifier.numeric(3);
const PARTITION_ID = 1;
const MESSAGES_PER_BATCH = 10;

// Iggy server address, defaults to 127.0.0.1:8090
const IGGY_SERVER_ADDRESS = process.env.IGGY_SERVER_ADDRESS || '127.0.0.1:8090';

// Message Types
const ORDER_CREATED_TYPE = 'order_created';
const ORDER_CONFIRMED_TYPE = 'order_confirmed';
const ORDER_REJECTED_TYPE = 'order_rejected';

// Helper to create an envelope for a message
const createEnvelope = (messageType, payload) => {
    return JSON.stringify({ messageType, payload });
};

// Sample message payloads
const createOrder = (id) => ({ id, currency: 'USD', amount: Math.random() * 100 });
const confirmOrder = (id) => ({ id, confirmationTime: new Date().toISOString() });
const rejectOrder = (id) => ({ id, reason: 'Insufficient stock' });

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function runExample() {
    const client = IggyClient.create(IGGY_SERVER_ADDRESS, { protocol: 'tcp' });
    console.log(`Connecting to Iggy server at ${IGGY_SERVER_ADDRESS}...`);
    await client.connect();
    console.log('Logging in...');
    await client.loginUser({ username: 'root', password: 'password' });

    // 1. Create Stream if it does not exist
    try {
        console.log(`Creating stream "envelope-stream" with ID: ${STREAM_ID.value}`);
        await client.createStream({ name: 'envelope-stream', streamId: STREAM_ID.value });
        console.log('Stream created successfully.');
    } catch (e) {
        if (e.code === 'stream_id_already_exists' || e.code === 'stream_name_already_exists') {
            console.log('Stream already exists, skipping creation.');
        } else {
            console.error('Error creating stream:', e);
            await client.disconnect();
            return;
        }
    }

    // 2. Create Topic if it does not exist
    try {
        console.log(`Creating topic "envelope-topic" with ID: ${TOPIC_ID.value}`);
        await client.createTopic({
            streamId: STREAM_ID,
            name: 'envelope-topic',
            partitionsCount: 1,
            compressionAlgorithm: CompressionAlgorithm.None,
            topicId: TOPIC_ID.value,
        });
        console.log('Topic created successfully.');
    } catch (e) {
        if (e.code === 'topic_id_already_exists' || e.code === 'topic_name_already_exists') {
            console.log('Topic already exists, skipping creation.');
        } else {
            console.error('Error creating topic:', e);
            await client.disconnect();
            return;
        }
    }

    // 3. Produce Messages with Envelopes
    console.log(`Sending ${MESSAGES_PER_BATCH} messages with envelopes...`);
    const messages = [];
    for (let i = 0; i < MESSAGES_PER_BATCH; i++) {
        let envelope;
        const orderId = `order-${i}`;
        const messageType = i % 3;

        if (messageType === 0) {
            envelope = createEnvelope(ORDER_CREATED_TYPE, createOrder(orderId));
        } else if (messageType === 1) {
            envelope = createEnvelope(ORDER_CONFIRMED_TYPE, confirmOrder(orderId));
        } else {
            envelope = createEnvelope(ORDER_REJECTED_TYPE, rejectOrder(orderId));
        }
        messages.push(new Message(Buffer.from(envelope)));
    }
    await client.sendMessages({
        streamId: STREAM_ID,
        topicId: TOPIC_ID,
        partitioning: Partitioning.partitionId(PARTITION_ID),
        messages: messages,
    });
    console.log(`Sent ${MESSAGES_PER_BATCH} messages.`);

    await sleep(1000); // Wait a bit for messages to be processed

    // 4. Consume Messages and handle based on envelope type
    console.log('\nPolling for messages...');
    const polled = await client.pollMessages({
        streamId: STREAM_ID,
        topicId: TOPIC_ID,
        partitionId: PARTITION_ID,
        pollingStrategy: PollingStrategy.offset(0),
        count: MESSAGES_PER_BATCH,
        autoCommit: false,
    });

    if (polled.messages.length > 0) {
        console.log(`Successfully polled ${polled.messages.length} messages.`);
        polled.messages.forEach(msg => {
            const envelope = JSON.parse(msg.payload.toString());
            console.log(`\nReceived message with offset: ${msg.offset}, type: ${envelope.messageType}`);
            switch (envelope.messageType) {
                case ORDER_CREATED_TYPE:
                    console.log('  -> Order Created:', envelope.payload);
                    break;
                case ORDER_CONFIRMED_TYPE:
                    console.log('  -> Order Confirmed:', envelope.payload);
                    break;
                case ORDER_REJECTED_TYPE:
                    console.log('  -> Order Rejected:', envelope.payload);
                    break;
                default:
                    console.warn(`Unknown message type: ${envelope.messageType}`);
            }
        });
    } else {
        console.log('No messages found.');
    }

    await client.disconnect();
    console.log('\nDisconnected from Iggy server.');
}

runExample().catch(e => {
    console.error('An error occurred:', e);
    process.exit(1);
});
