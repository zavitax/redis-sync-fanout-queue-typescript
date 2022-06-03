import Redis from 'ioredis';
import { RedisScriptCall, prepare_redis_script, redis_call, clone_redis_connection } from './redis-utils';
import * as scripts from './lua-scripts';

export interface RedisQueueWireMessage {
  t: number;
  c: string;
  s: number;
  d: any;
}

export type HandleMessageAckCall = () => Promise<void>;

export interface HandleMessageCallArguments {
  data: any,
  ack: HandleMessageAckCall | null,
  context: {
    timestamp: number | null,
    sequence: number | null,
  }
}

// export type ClientNoticeCall = ({ clientId, room }: { clientId: string, room: string }) => void;
export type RoomNoticeCall = ({ room }: { room: string }) => void;
export type HandleMessageCall = ({ data, context }: HandleMessageCallArguments) => void;

export interface ConstructorArgs {
  redis: Redis;
  sync: boolean;
  clientTimeoutMs: number;
  redisKeyPrefix: string;
  handleRoomEjected: RoomNoticeCall | null;
}

interface Room {
  handleMessage: HandleMessageCall;
};

export class RedisQueueClient {
  private redis: Redis;
  private redis_subscriber_timeout: Redis;
  private redis_subscriber_message: Redis;
  private clientTimeoutMs: number;
  private redisKeyPrefix: string;

  private handleRoomEjected: RoomNoticeCall | null = null;

  private clientId: string;
  private lastMessageSequenceNumber: number = 0;

  private statTotalInvalidMessagesCount: number = 0;
  private statLastMessageLatencies: number[] = [];
  private statLastMessageReceiveTimestamps: number[] = [];

  private callCreateClientID: RedisScriptCall;
  private callUpdateClientTimestamp: RedisScriptCall;
  private callAddSyncClientToRoom: RedisScriptCall;
  private callRemoveSyncClientFromRoom: RedisScriptCall;
  //private callRemoveTimedOutClients: RedisScriptCall;
  private callConditionalProcessRoomMessages: RedisScriptCall;
  private callEnqueueRoomMessage: RedisScriptCall;
  private callAckClientMessage: RedisScriptCall;

  private redisScriptsPrepared: boolean = false;

  private rooms: Record<string, Room> = {};

  private housekeepingIntervalHandle: NodeJS.Timer;

  private sync: boolean = true;

  private _handleMessageStats (wireMessage: RedisQueueWireMessage): void {
    const now = Date.now();
    const latencyMs = Math.max(0, now - wireMessage.t);

    this.statLastMessageLatencies.push(latencyMs);
    while (this.statLastMessageLatencies.length > 100) { this.statLastMessageLatencies.shift(); }

    this.statLastMessageReceiveTimestamps.push(now);
    while (this.statLastMessageReceiveTimestamps.length > 100) { this.statLastMessageReceiveTimestamps.shift(); }
  }

  private keyClientIDSequence: string;
  private keyLastTimestamp: string;
  private keyGlobalSetOfKnownClients: string;
  private keyPubsubAdminEventsRemoveClientTopic: string;
  private keyGlobalKnownRooms: string;

  private keyRoomQueue(room: string): string {
    return `${this.redisKeyPrefix}::room::${room}::msg-queue`;
  }
  
  private keyRoomPubsub(room: string): string {
    return `${this.redisKeyPrefix}::room::${room}::msg-pubsub`;
  }

  private keyRoomSetOfKnownClients(room: string): string {
    return `${this.redisKeyPrefix}::room::${room}::known-clients`;
  };

  private keyRoomSetOfAckedClients(room: string): string {
    return `${this.redisKeyPrefix}::room::${room}::acked-clients`;
  }

  private keyGlobalLock(tag: string): string {
    return `${this.redisKeyPrefix}::lock::${tag}`;
  }

  constructor ({
    redis,
    clientTimeoutMs = 30000,
    sync = true,
    redisKeyPrefix = '{redis-ordered-queue}',
    handleRoomEjected = null,
  }: ConstructorArgs) {
    this.redis = redis;
    this.clientTimeoutMs = clientTimeoutMs;
    this.sync = sync;
    this.redisKeyPrefix = redisKeyPrefix;

    this.handleRoomEjected = handleRoomEjected;

    this.keyClientIDSequence = `${redisKeyPrefix}::global::last-client-id-seq`;
    this.keyLastTimestamp = `${redisKeyPrefix}::global::last-client-id-timestamp`;
    this.keyGlobalSetOfKnownClients = `${redisKeyPrefix}::global::known-clients`;
    this.keyPubsubAdminEventsRemoveClientTopic = `${redisKeyPrefix}::global::pubsub::admin::removed-clients`;
    this.keyGlobalKnownRooms = `${redisKeyPrefix}::global::known-rooms`;

    this.housekeepingIntervalHandle = setInterval(this._housekeep.bind(this), Math.ceil(this.clientTimeoutMs / 2));
  }

  async subscribe ({
    room,
    handleMessage,
  }: { room: string, handleMessage: HandleMessageCall }) {   
    room = room.toLowerCase();

    await this._initRedis();

    await this._removeTimedOutClients();
    await this._conditionalProcessRoomMessages(room);

    await this.redis_subscriber_message.subscribe(this.keyRoomPubsub(room));

    await this._subscribe(room, handleMessage);

    if (this.sync) {
      await this._pong();
    }
  }

  async unsubscribe ({ room }: { room: string }) {
    room = room.toLowerCase();

    await this._initRedis();

    await this.redis_subscriber_message.unsubscribe(this.keyRoomPubsub(room));

    await this._unsubscribe(room);
    await this._removeTimedOutClients();
    await this._conditionalProcessRoomMessages(room);
  }

  async send ({ room, priority, data }: { room: string, priority: number, data: any }): Promise<void> {
    room = room.toLowerCase();

    await this._initRedis();

    await this._pong();
    await this._removeTimedOutClients();
    await this._send({ room, priority, data });
    await this._conditionalProcessRoomMessages(room);
  }

  async sendOutOfBand ({ room, data }: { room: string, data: any }): Promise<void> {
    room = room.toLowerCase();

    await this._initRedis();

    await this._removeTimedOutClients();
    await this._sendOutOfBand({ room, data });
    await this._conditionalProcessRoomMessages(room);
  }

  async ack (token: string): Promise<void> {
    const [ _ /*clientId*/, room ] = token.split('::', 2);

    await this._initRedis();

    await this._pong();
    await this._ack({ room });
    await this._removeTimedOutClients();
    await this._conditionalProcessRoomMessages(room);
  }

  async pong (): Promise<void> {
    await this._initRedis();

    await this._pong();
    await this._removeTimedOutClients();
    await this._conditionalProcessRoomsMessages();
  }

  private async _housekeep (): Promise<void> {
    await this._initRedis();

    await this._removeTimedOutClients();
    await this._conditionalProcessRoomsMessages();
  }

  async peek ({ room, limit = 1 }: { room: string, limit: number }): Promise<any[]> {
    await this._initRedis();

    const msgs = await this._peek(room, 0, limit);

    return msgs.map(msg => {
      const wire = JSON.parse(msg) as RedisQueueWireMessage;
      
      return wire.d;
    })
  }

  private async _lock (tag: string, timeoutMs: number = 30000): Promise<boolean> {
    const result = await redis_call(this.redis, 'SET', this.keyGlobalLock(tag), this.clientId, 'NX', 'PX', timeoutMs);

    return !!result;
  }

  /*
  private async _unlock (tag: string): Promise<void> {
    await redis_call(this.redis, 'DEL', this.keyGlobalLock(tag));
  }*/

  private async _peek (room: string, offset: number, limit: number): Promise<string[]> {
    room = room.toLowerCase();

    return await redis_call(this.redis, 'ZRANGEBYSCORE', this.keyRoomQueue(room), "-inf", "+inf", 'LIMIT', offset, limit);
  }

  private async _ack ({ room }: { room: string }): Promise<void> {
    room = room.toLowerCase();

    this.callAckClientMessage(
      [ room, this.clientId, Date.now() ],
      [ this.keyRoomSetOfKnownClients(room), this.keyRoomSetOfAckedClients(room), this.keyGlobalKnownRooms, this.keyRoomQueue(room), this.keyRoomPubsub(room) ]
    );
  }

  private _handleTimeoutMessage (_channel: string, message: string): void {
    const [ clientId, _ ] = message.split('::', 2);

    if (clientId === this.clientId) {
      if (this.handleRoomEjected) {
        for (const room of Object.keys(this.rooms)) {
          this.redis_subscriber_message.unsubscribe(this.keyRoomPubsub(room));
          
          this.handleRoomEjected({ room });
        }
      }

      this.rooms = {};
    }
  }

  private async _handleRoomMessage(channel: string, message: string): Promise<void> {
    const parts = channel.split('::');

    if (parts[parts.length - 1] === 'msg-pubsub') {
      const room = parts[parts.length - 2];

      await this._handleMessage(room, message);
    }
}

  private async _initRedis (): Promise<void> {
    if (this.redisScriptsPrepared) return;

    this._handleTimeoutMessage = this._handleTimeoutMessage.bind(this);
    this._handleRoomMessage = this._handleRoomMessage.bind(this);

    this.redis_subscriber_timeout = await clone_redis_connection(this.redis);
    this.redis_subscriber_message = await clone_redis_connection(this.redis);

    this.redis_subscriber_timeout.on('message', this._handleTimeoutMessage);
    this.redis_subscriber_message.on('message', this._handleRoomMessage);
   
    this.callCreateClientID = await prepare_redis_script(this.redis, scripts.CreateClientID, 'CreateClientID');
    this.callUpdateClientTimestamp = await prepare_redis_script(this.redis, scripts.UpdateClientTimestamp, 'UpdateClientTimestamp');
    this.callAddSyncClientToRoom = await prepare_redis_script(this.redis, scripts.AddSyncClientToRoom, 'AddSyncClientToRoom');
    this.callRemoveSyncClientFromRoom = await prepare_redis_script(this.redis, scripts.RemoveSyncClientFromRoom, 'RemoveSyncClientFromRoom');
    //this.callRemoveTimedOutClients = await prepare_redis_script(this.redis, scripts.RemoveTimedOutClients, 'RemoveTimedOutClients');
    this.callConditionalProcessRoomMessages = await prepare_redis_script(this.redis, scripts.ConditionalProcessRoomMessages, 'ConditionalProcessRoomMessages');
    this.callEnqueueRoomMessage = await prepare_redis_script(this.redis, scripts.EnqueueRoomMessage, 'EnqueueRoomMessage');
    this.callAckClientMessage = await prepare_redis_script(this.redis, scripts.AckClientMessage, 'AckClientMessage');

    this.clientId = await this.callCreateClientID(
      [ Date.now() ],
      [ this.keyClientIDSequence, this.keyLastTimestamp ]);

    //await redis_call(this.redis_subscriber_timeout, 'SUBSCRIBE', this.keyPubsubAdminEventsRemoveClientTopic);
    //await this.redis_subscriber_timeout.call('SUBSCRIBE', [ this.keyPubsubAdminEventsRemoveClientTopic ]);
    await this.redis_subscriber_timeout.subscribe(this.keyPubsubAdminEventsRemoveClientTopic);

    this.redisScriptsPrepared = true;
  }

  private async _subscribe (room: string, handleMessage: HandleMessageCall): Promise<void> {
    room = room.toLowerCase();

    if (room in this.rooms) throw new Error(`Already subscribed to room '${room}'`);

    if (this.sync) {
      const result = await this.callAddSyncClientToRoom(
        [ this.clientId, room, Date.now() ],
        [ this.keyGlobalSetOfKnownClients, this.keyRoomSetOfKnownClients(room), this.keyRoomSetOfAckedClients(room) ]
      );

      if (result) throw new Error(result);
    }

    this.rooms[room] = { handleMessage };
  }

  private async _unsubscribe (room: string): Promise<void> {
    room = room.toLowerCase();

    if (this.sync) {
      await this.callRemoveSyncClientFromRoom(
        [ this.clientId, room, Date.now() ],
        [ this.keyGlobalSetOfKnownClients, this.keyRoomSetOfKnownClients(room), this.keyRoomSetOfAckedClients(room), this.keyPubsubAdminEventsRemoveClientTopic ]
      );
    }
    
    delete this.rooms[room];
  }

  private async _pong (): Promise<void> {
    for (const room of Object.keys(this.rooms)) {
      await this.callUpdateClientTimestamp(
        [ this.clientId, room, Date.now() ],
        [ this.keyGlobalSetOfKnownClients, this.keyRoomSetOfKnownClients(room) ]
      )
    }
  }

  private async _removeTimedOutClients (): Promise<void> {
    if (!await this._lock('_removeTimedOutClients', 30000)) return;

    const argMaxTimestampToRemove = Date.now() - this.clientTimeoutMs;

    const clientRoomIDs = await redis_call(this.redis, 'ZRANGEBYSCORE', this.keyGlobalSetOfKnownClients, "-inf", argMaxTimestampToRemove);

    for (const clientRoomID of clientRoomIDs ) {
      const [ clientId, room ] = clientRoomID.split('::', 2);

      await this.callRemoveSyncClientFromRoom(
        [ clientId, room, Date.now() ],
        [ this.keyGlobalSetOfKnownClients, this.keyRoomSetOfKnownClients(room), this.keyRoomSetOfAckedClients(room), this.keyPubsubAdminEventsRemoveClientTopic ]
      )
    }
  }

  private async _conditionalProcessRoomsMessages (): Promise<void> {
    if (!await this._lock('_conditionalProcessRoomsMessages', 5000)) return;

    const roomIDs = await redis_call(this.redis, 'ZRANGEBYSCORE', this.keyGlobalKnownRooms, "-inf", "+inf");

    for (const room of roomIDs) {
      await this._conditionalProcessRoomMessages(room);
    }
  }

  private async _conditionalProcessRoomMessages (room: string): Promise<void> {
    await this.callConditionalProcessRoomMessages(
      [ room ],
      [ this.keyRoomSetOfKnownClients(room), this.keyRoomSetOfAckedClients(room), this.keyGlobalKnownRooms, this.keyRoomQueue(room), this.keyRoomPubsub(room) ]
    )
  }

  private async _handleMessage(room: string, message: string): Promise<void> {
    if (!this.rooms[room]) return;

    const wireMessage = JSON.parse(message) as RedisQueueWireMessage;

    const ack = (this.sync && !!wireMessage.s) ? async (): Promise<void> => {
      const ackToken = `${wireMessage.c}::${room}`;

      await this.ack(ackToken);
    } : null;

    const args: HandleMessageCallArguments = {
      data: wireMessage.d,
      ack: ack,
      context: {
        timestamp: wireMessage.t,
        sequence: wireMessage.s,
      }
    }

    this.rooms[room].handleMessage(args);
  }

  private async _send({ data, priority = 1, room }: { data: any, priority: number, room: string }): Promise<number> {
    const wireMessage: RedisQueueWireMessage = {
      t: Date.now(),
      c: this.clientId,
      s: ++this.lastMessageSequenceNumber,
      d: data
    };

    const msg = JSON.stringify(wireMessage);

    await this.callEnqueueRoomMessage(
      [ room, priority, msg ],
      [ this.keyRoomSetOfKnownClients(room), this.keyGlobalKnownRooms, this.keyRoomQueue(room) ]
    );

    return 1;
  }

  private async _sendOutOfBand({ data, room }: { data: any, room: string }): Promise<number> {
    const wireMessage: RedisQueueWireMessage = {
      t: Date.now(),
      c: this.clientId,
      s: 0,
      d: data
    };

    const msg = JSON.stringify(wireMessage);

    await redis_call(this.redis, 'PUBLISH', this.keyRoomPubsub(room), msg);

    return 1;
  }

  async dispose (): Promise<void> {
    clearInterval(this.housekeepingIntervalHandle);

    for (const room of Object.keys(this.rooms)) {
      await this._unsubscribe(room);
    }

    this.redis_subscriber_timeout.off('message', this._handleTimeoutMessage);
    this.redis_subscriber_message.off('message', this._handleRoomMessage);

    await this.redis_subscriber_timeout.unsubscribe(this.keyPubsubAdminEventsRemoveClientTopic);

    await this.redis_subscriber_timeout.disconnect(false);
    await this.redis_subscriber_message.disconnect(false);
  }
}
