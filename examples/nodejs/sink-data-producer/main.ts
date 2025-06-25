import { IggyClient, Identifier, Partitioning, Message } from '@iggy.rs/sdk';

// Define an interface for the record structure for type safety
interface Record {
    user_id: string;
    user_type: number;
    email: string;
    source: string;
    state: string;
    created_at: string;
    message: string;
}

const IGGY_ADDRESS: string = process.env.IGGY_ADDRESS || 'localhost:8090';
const IGGY_USERNAME: string = process.env.IGGY_USERNAME || 'root';
const IGGY_PASSWORD: string = process.env.IGGY_PASSWORD || 'password';
const IGGY_STREAM_ID: Identifier = Identifier.numeric(Number(process.env.IGGY_STREAM_ID) || 110);
const IGGY_TOPIC_ID: Identifier = Identifier.numeric(Number(process.env.IGGY_TOPIC_ID) || 110);

const SOURCES: string[] = ['browser', 'mobile', 'desktop', 'email', 'network', 'other'];
const STATES: string[] = ['active', 'inactive', 'blocked', 'deleted', 'unknown'];
const DOMAINS: string[] = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com'];

function getRandomElement<T>(arr: T[]): T {
    return arr[Math.floor(Math.random() * arr.length)];
}

function getRandomInt(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomString(size: number): string {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < size; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

function randomRecord(): Record {
    const createdAt = new Date();
    createdAt.setDate(createdAt.getDate() - getRandomInt(0, 1000));

    return {
        user_id: `user_${getRandomInt(1, 100)}`,
        user_type: getRandomInt(1, 5),
        email: `${randomString(getRandomInt(3, 20))}@${getRandomElement(DOMAINS)}`,
        source: getRandomElement(SOURCES),
        state: getRandomElement(STATES),
        message: randomString(getRandomInt(10, 100)),
        created_at: createdAt.toISOString(),
    };
}

async function runDataProducer(): Promise<void> {
    console.log('Starting data producer...');
    const client = IggyClient.create(IGGY_ADDRESS, { protocol: 'tcp' });
    await client.connect();
    await client.loginUser({ username: IGGY_USERNAME, password: IGGY_PASSWORD });
    console.log(`Connected to Iggy at ${IGGY_ADDRESS}`);

    try {
        await client.createStream({ name: 'data-stream', streamId: IGGY_STREAM_ID.value });
        console.log(`Stream with ID '${IGGY_STREAM_ID.value}' created or already exists.`);
    } catch (e: any) {
        if (e.code !== 'stream_id_already_exists' && e.code !== 'stream_name_already_exists') throw e;
        console.log(`Stream with ID '${IGGY_STREAM_ID.value}' already exists.`);
    }
    try {
        await client.createTopic({ streamId: IGGY_STREAM_ID, name: 'records-topic', partitionsCount: 1, topicId: IGGY_TOPIC_ID.value });
        console.log(`Topic with ID '${IGGY_TOPIC_ID.value}' created or already exists.`);
    } catch (e: any) {
        if (e.code !== 'topic_id_already_exists' && e.code !== 'topic_name_already_exists') throw e;
        console.log(`Topic with ID '${IGGY_TOPIC_ID.value}' already exists.`);
    }

    let batchesCount = 0;
    while (batchesCount < 100) {
        const recordsCount = getRandomInt(100, 500);
        const messages: Message[] = Array.from({ length: recordsCount }, () => {
            const record = randomRecord();
            const payload = JSON.stringify(record);
            return new Message(Buffer.from(payload));
        });

        try {
            await client.sendMessages({
                streamId: IGGY_STREAM_ID,
                topicId: IGGY_TOPIC_ID,
                partitioning: Partitioning.balanced(),
                messages,
            });
            console.log(`Sent batch #${batchesCount + 1} with ${recordsCount} messages.`);
            batchesCount++;
        } catch (e) {
            console.error('Error sending messages:', e);
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }

    console.log('Reached maximum batches count (100). Producer finished.');
    await client.disconnect();
}

runDataProducer().catch(e => {
    console.error('\nAn error occurred:', e);
    process.exit(1);
});
