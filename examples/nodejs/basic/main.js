import { IggyClient, Identifier, Partitioning, PollingStrategy, Message, CompressionAlgorithm } from '@iggy.rs/sdk';

const STREAM_ID = Identifier.numeric(2);
const TOPIC_ID = Identifier.numeric(2);
const PARTITION_ID = 1;
const MESSAGES_COUNT = 100;

// Iggy server address, defaults to 127.0.0.1:8090
const IGGY_SERVER_ADDRESS = process.env.IGGY_SERVER_ADDRESS || '127.0.0.1:8090';

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
        console.log(`Creating stream "basic-stream" with ID: ${STREAM_ID.value}`);
        await client.createStream({ name: 'basic-stream', streamId: STREAM_ID.value });
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
        console.log(`Creating topic "basic-topic" with ID: ${TOPIC_ID.value}`);
        await client.createTopic({
            streamId: STREAM_ID,
            name: 'basic-topic',
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

    // 3. Produce Messages
    console.log(`Sending ${MESSAGES_COUNT} messages...`);
    const messages = [];
    for (let i = 0; i < MESSAGES_COUNT; i++) {
        const payload = `message-${i}`;
        messages.push(new Message(Buffer.from(payload)));
    }
    await client.sendMessages({
        streamId: STREAM_ID,
        topicId: TOPIC_ID,
        partitioning: Partitioning.partitionId(PARTITION_ID),
        messages: messages,
    });
    console.log(`Sent ${MESSAGES_COUNT} messages.`);

    await sleep(1000); // Wait a bit for messages to be processed

    // 4. Consume Messages
    console.log('\nPolling for messages...');
    const polled = await client.pollMessages({
        streamId: STREAM_ID,
        topicId: TOPIC_ID,
        partitionId: PARTITION_ID,
        pollingStrategy: PollingStrategy.offset(0),
        count: MESSAGES_COUNT,
        autoCommit: false,
    });

    if (polled.messages.length > 0) {
        console.log(`Successfully polled ${polled.messages.length} messages.`);
        polled.messages.forEach(msg => {
            console.log(`  - Polled message with offset: ${msg.offset}, payload: ${msg.payload.toString()}`);
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
