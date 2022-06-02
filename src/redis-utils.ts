import Redis from 'ioredis';

export type RedisArgumentType = string | number;
export type RedisScriptCall = (args: RedisArgumentType[], keys: string[]) => Promise<any>;

export async function prepare_redis_script(redis: Redis, script: string, label: string): Promise<RedisScriptCall> {
  const hash: string = await redis_call(redis, 'SCRIPT', 'LOAD', script) as string;

  return async (args: RedisArgumentType[], keys: string[]): Promise<any> => {
    try {
      return await redis_call(redis, 'EVALSHA', hash, keys.length, ...keys, ...args);
    } catch (err) {
      console.error(label, 'EVALSHA ', hash, keys.length, ...keys, ...args);
      throw err;
    }
  }; 
}

export async function clone_redis_connection(redis: Redis): Promise<Redis> {
  const copy = redis.duplicate({
    lazyConnect: true
  });

  await copy.connect();

  return copy;
}

export async function create_client_id(redis: Redis, clientIndexKey: string): Promise<number> {
  return await redis_call(redis, 'INCR', clientIndexKey) as number;
}

export async function redis_call(redis: Redis, command: string, ...args: any[]): Promise<any> {
  return redis.call(command, [ ...args ]);
}
