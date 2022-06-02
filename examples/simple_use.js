//const { RedisQueueClient } = require('redis-sync-fanout-queue');
const { RedisQueueClient } = require('../dist');
const Redis = require('ioredis');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
  const redis = new Redis({
    host: 'localhost',
    port: 6379,
    lazyConnect: true,
    showFriendlyErrorStack: true,
  });
  
  await redis.connect();

  await redis.send_command('FLUSHDB');

  const client = new RedisQueueClient({
    redis: redis,
    clientTimeoutMs: 10000,
    redisKeyPrefix: `redis-sync-fanout-queue`
  });

  console.log('send');

  await client.subscribe({
    room: 'room1',
    handleMessage: async ({ data, ack }) => {
      console.log('Message: ', 'room1', ': ', ack, ' -> ', data);

      if (!!ack) {
        await ack();
      }
    }
  });

  for (let i = 0; i < 10; ++i) {
    console.log('send ', i);
    await client.send({
      data: { my: `message ${i}` },
      priority: 1,
      room: 'room1'
    });
  }

  await sleep(60000 * 5);
  console.log('Shutting down...');
  await client.unsubscribe({ room: 'room1' });

  await client.dispose();

  await redis.quit();
}

main();
