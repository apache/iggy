import { IggyClient, Identifier, Partitioning, PollingStrategy, Message } from '@iggy.rs/sdk';

const IGGY_SERVER_ADDRESS = process.env.IGGY_SERVER_ADDRESS || '127.0.0.1:8090';
const STREAM_ID = Identifier.numeric(1001);
const TOPIC_ID = Identifier.numeric(1001);
const PARTITION_ID = 1;

async function runExample() {
    console.log('Stream Basic example started...');

    const client = IggyClient.create(IGGY_SERVER_ADDRESS, { protocol: 'tcp' });
    await client.connect();
    await client.loginUser({ username: 'root', password: 'password' });
    console.log('Connected and logged in to Iggy server.');

    try {
        // 1. Create a stream and a topic
        await client.createStream({ name: 'basic-stream', streamId: STREAM_ID.value });
        console.log(`Stream with ID '${STREAM_ID.value}' created.`);
        await client.createTopic({ streamId: STREAM_ID, topicId: TOPIC_ID.value, partitionsCount: 1, name: 'basic-topic' });
        console.log(`Topic with ID '${TOPIC_ID.value}' created.`);

        // 2. Send a few messages
        console.log('Sending 3 messages...');
        const messages = [
            new Message(Buffer.from('Hello World')),
            new Message(Buffer.from('Hola Iggy')),
            new Message(Buffer.from('Hi Apache'))
        ];
        await client.sendMessages({
            streamId: STREAM_ID,
            topicId: TOPIC_ID,
            partitioning: Partitioning.partitionId(PARTITION_ID),
            messages
        });
        console.log('Messages sent successfully.');

        // 3. Poll the messages
        const polledMessages = await client.pollMessages({
            streamId: STREAM_ID,
            topicId: TOPIC_ID,
            partitionId: PARTITION_ID,
            pollingStrategy: PollingStrategy.offset(0),
            count: 3,
            autoCommit: false
        });

        console.log('Polled messages:');
        for (const message of polledMessages.messages) {
            console.log(`- ${message.payload.toString()}`);
        }

    } catch (e) {
        console.error('An error occurred:', e);
    } finally {
        // 4. Clean up the stream
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
