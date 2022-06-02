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
    redisKeyPrefix: `redis-sync-fanout-queue`
  });

  await client.subscribe({
    room: 'room1',
    handleMessage: async ({ data, ack }) => {
      console.log('Message: ', 'room1', ': ', ack, ' -> ', data);

      if (!!ack) {
        setTimeout(async () => {
          await ack();
        }, Math.ceil(Math.random() * 1000))
      }
    }
  });

  setInterval(() => {
    client.pong();
  }, 3000);
}

main();
