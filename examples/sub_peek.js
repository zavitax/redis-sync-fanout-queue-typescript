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
    redisKeyPrefix: `redis-sync-fanout-queue`,
    handleRoomEjected: ({ room }) => {
      console.log('Ejected from room: ', room);
    },
  });

  await client.subscribe({
    room: 'room1',
    handleMessage: ({ data, ack }) => {
      console.log('Message: ', 'room1', ': ', ack, ' -> ', data);

      if (!!ack) {
        setTimeout(async () => {
          await ack();
        }, Math.ceil(Math.random() * 1000))
      }
    }
  });

  setInterval(async () => {
    await client.pong();
    const msgs = await client.peek({ room: 'room1', limit: 10 });
    console.log(msgs);
  }, 3000);
}

main();
