import { IggyClient, Identifier, Partitioning, PollingStrategy, Message } from '@iggy.rs/sdk';

const IGGY_SERVER_ADDRESS = process.env.IGGY_SERVER_ADDRESS || '127.0.0.1:8090';
const STREAM_ID = Identifier.numeric(101);
const TOPIC_ID = Identifier.numeric(101);
const PARTITION_ID = 1;
const MESSAGES_COUNT = 1000;

async function runExample() {
    console.log('Basic example started...');

    const client = IggyClient.create(IGGY_SERVER_ADDRESS, { protocol: 'tcp' });
    await client.connect();
    await client.loginUser({ username: 'root', password: 'password' });
    console.log('Connected and logged in to Iggy server.');

    try {
        await client.createStream({ name: 'basic-stream', streamId: STREAM_ID.value });
        console.log(`Stream with ID '${STREAM_ID.value}' created.`);
        await client.createTopic({ streamId: STREAM_ID, topicId: TOPIC_ID.value, partitionsCount: 1, name: 'basic-topic' });
        console.log(`Topic with ID '${TOPIC_ID.value}' created.`);

        console.log(`Sending ${MESSAGES_COUNT} messages...`);
        const messages: Message[] = [];
        for (let i = 1; i <= MESSAGES_COUNT; i++) {
            const payload = Buffer.from(`message-${i}`);
            messages.push(new Message(payload));
        }
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
            count: MESSAGES_COUNT,
            autoCommit: false
        });

        if (polledMessages.messages.length === MESSAGES_COUNT) {
            console.log(`Successfully polled ${polledMessages.messages.length} messages.`);
        } else {
            console.error(`Failed to poll messages, expected ${MESSAGES_COUNT}, but got ${polledMessages.messages.length}`);
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
