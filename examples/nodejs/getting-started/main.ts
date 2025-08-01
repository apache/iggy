import { IggyClient, Identifier, Partitioning, PollingStrategy, Message, Stream, Topic } from '@iggy.rs/sdk';

const IGGY_SERVER_ADDRESS = process.env.IGGY_SERVER_ADDRESS || '127.0.0.1:8090';
const STREAM_ID = Identifier.numeric(100);
const TOPIC_ID = Identifier.numeric(100);
const PARTITION_ID = 1;
const MESSAGES_COUNT = 10;

async function runExample() {
    console.log('Getting Started example started...');

    const client = IggyClient.create(IGGY_SERVER_ADDRESS, { protocol: 'tcp' });
    await client.connect();
    await client.loginUser({ username: 'root', password: 'password' });
    console.log('Connected and logged in to Iggy server.');

    try {
        console.log(`Creating stream with ID '${STREAM_ID.value}'...`);
        await client.createStream({ name: 'sample-stream', streamId: STREAM_ID.value });
        console.log(`Stream with ID '${STREAM_ID.value}' created.`);

        const stream = await client.getStream({ streamId: STREAM_ID });
        console.log('Stream details:', stream);

        console.log(`Creating topic with ID '${TOPIC_ID.value}'...`);
        await client.createTopic({ streamId: STREAM_ID, topicId: TOPIC_ID.value, partitionsCount: 1, name: 'sample-topic' });
        console.log(`Topic with ID '${TOPIC_ID.value}' created.`);

        const topic = await client.getTopic({ streamId: STREAM_ID, topicId: TOPIC_ID });
        console.log('Topic details:', topic);

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

        console.log('Polling messages...');
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
        console.log(`Deleting stream with ID '${STREAM_ID.value}'...`);
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
