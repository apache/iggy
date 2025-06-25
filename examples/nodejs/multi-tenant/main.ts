import { IggyClient, Identifier, Partitioning, PollingStrategy, Message, CompressionAlgorithm, Permissions } from '@iggy.rs/sdk';

// Interface to hold tenant-specific information
interface Tenant {
    id: string;
    streamId: Identifier;
    streamName: string;
    userName: string;
}

const IGGY_SERVER_ADDRESS = process.env.IGGY_SERVER_ADDRESS || '127.0.0.1:8090';
const TENANTS_COUNT = 2;
const TOPICS = ['events', 'logs'];
const PASSWORD = 'password';

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function runExample() {
    console.log('Multi-tenant example started...');

    // 1. Setup: Connect as root user to create tenants (streams, users, permissions)
    const rootClient = IggyClient.create(IGGY_SERVER_ADDRESS, { protocol: 'tcp' });
    await rootClient.connect();
    await rootClient.loginUser({ username: 'root', password: 'password' });
    console.log('\nRoot client connected and logged in.');

    const tenants: Tenant[] = [];
    for (let i = 1; i <= TENANTS_COUNT; i++) {
        const tenantId = `tenant_${i}`;
        const streamId = Identifier.numeric(100 + i);
        const streamName = `${tenantId}_stream`;
        const userName = `${tenantId}_user`;

        console.log(`\nSetting up for ${tenantId}...`);

        // Create stream
        try {
            await rootClient.createStream({ name: streamName, streamId: streamId.value });
            console.log(`- Stream '${streamName}' created.`);
        } catch (e: any) {
            if (e.code !== 'stream_id_already_exists' && e.code !== 'stream_name_already_exists') throw e;
            console.log(`- Stream '${streamName}' already exists.`);
        }

        // Create topics
        for (const topicName of TOPICS) {
            const topicId = Identifier.string(topicName);
            try {
                await rootClient.createTopic({ streamId, name: topicName, partitionsCount: 1, compressionAlgorithm: CompressionAlgorithm.None, topicId: topicId.value });
                console.log(`- Topic '${topicName}' created in stream '${streamName}'.`);
            } catch (e: any) {
                if (e.code !== 'topic_id_already_exists' && e.code !== 'topic_name_already_exists') throw e;
                console.log(`- Topic '${topicName}' in stream '${streamName}' already exists.`);
            }
        }

        // Create user
        try {
            await rootClient.createUser({ username: userName, password: PASSWORD, permissions: Permissions.default(streamId.value) });
            console.log(`- User '${userName}' created with default permissions for stream ${streamId.value}.`);
        } catch (e: any) {
            if (e.code !== 'user_already_exists') throw e;
            console.log(`- User '${userName}' already exists.`);
        }
        tenants.push({ id: tenantId, streamId, streamName, userName });
    }
    await rootClient.disconnect();
    console.log('\nRoot client disconnected. Setup complete.');

    // 2. Tenant Operations: Connect as each tenant and perform actions
    for (const tenant of tenants) {
        console.log(`\n--- Operating as ${tenant.id} ---`);
        const tenantClient = IggyClient.create(IGGY_SERVER_ADDRESS, { protocol: 'tcp' });
        await tenantClient.connect();
        await tenantClient.loginUser({ username: tenant.userName, password: PASSWORD });
        console.log(`- Logged in as ${tenant.userName}.`);

        // Produce messages to own stream
        const topicId = Identifier.string(TOPICS[0]);
        const message = new Message(Buffer.from(`Hello from ${tenant.id}`));
        await tenantClient.sendMessages({
            streamId: tenant.streamId,
            topicId,
            partitioning: Partitioning.partitionId(1),
            messages: [message]
        });
        console.log(`- Sent message to own stream '${tenant.streamName}'.`);

        // Consume messages from own stream
        await sleep(500);
        const polled = await tenantClient.pollMessages({
            streamId: tenant.streamId,
            topicId,
            partitionId: 1,
            pollingStrategy: PollingStrategy.offset(0),
            count: 1,
            autoCommit: false
        });
        console.log(`- Polled message: '${polled.messages[0].payload.toString()}' from own stream.`);

        // Attempt to access another tenant's stream (should fail)
        const otherTenant = tenants.find(t => t.id !== tenant.id)!;
        try {
            await tenantClient.getStream({ streamId: otherTenant.streamId });
            console.error(`- [FAIL] ${tenant.id} SHOULD NOT have access to ${otherTenant.streamName}.`);
        } catch (e) {
            console.log(`- [SUCCESS] Access to stream '${otherTenant.streamName}' was correctly denied.`);
        }

        await tenantClient.disconnect();
        console.log(`- Disconnected as ${tenant.userName}.`);
    }

    // 3. Cleanup: Connect as root to delete streams and users
    console.log('\n--- Cleanup Phase ---');
    await rootClient.connect();
    await rootClient.loginUser({ username: 'root', password: 'password' });
    console.log('Root client reconnected for cleanup.');

    for (const tenant of tenants) {
        await rootClient.deleteStream({ streamId: tenant.streamId });
        console.log(`- Deleted stream '${tenant.streamName}'.`);
        await rootClient.deleteUser({ userId: Identifier.string(tenant.userName) });
        console.log(`- Deleted user '${tenant.userName}'.`);
    }

    await rootClient.disconnect();
    console.log('\nCleanup complete. Example finished.');
}

runExample().catch(e => {
    console.error('\nAn error occurred:', e);
    process.exit(1);
});
