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

  const client = new RedisQueueClient({
    redis: redis,
    clientTimeoutMs: 10000,
    sync: true,
    redisKeyPrefix: `redis-sync-fanout-queue`
  });

  for (let i = 0; i < 10; ++i) {
    if (i % 1000 === 0) {
      console.log('send ', i);
    }
    await client.send({
      data: { my: `message ${i}` },
      priority: 1,
      room: 'room1'
    });
  }

  await sleep(1000);
  console.log('Shutting down...');

  await client.dispose();

  await redis.quit();
}

main();
