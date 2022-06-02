# redis-sync-fanout-queue

## What is it?

Priority queue with synchronous fanout delivery for each room based on Redis

## Queue guarantees

### Low latency delivery

Delivery is based on Redis PUBSUB. It is possible to reach very low latencies.

### Synchronized Fanout

All synchronous clients must ACKnowledge processing of a message before any other client can see the next message.

### At most once delivery

There are no message redelivery attempts built in. Either you get it or you do not.

### High performance, low memory footprint

Most of the heavy lifting is done in Redis.

## Infrastructure

The library leverages `ioredis` for communication with the Redis server.

The expected version of `ioredis` API is 5.x.

## Installation

```sh
npm i redis-sync-fanout-queue
```

## Usage

```typescript
const { RedisQueueClient } = require('redis-sync-fanout-queue');
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
```
