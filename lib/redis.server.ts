import { Server, CustomTransportStrategy } from '@nestjs/microservices';
import { RedisEventsMap } from '@nestjs/microservices/events/redis.events';
import {
  ConstructorOptions,
  RedisInstance,
  RedisStreamPattern,
  StreamResponse,
  StreamResponseObject,
} from './interfaces';

import { createRedisConnection } from './redis.utils';
// ioredis event names used internally
import { deserialize, serialize } from './streams.utils';
import { RedisStreamContext } from './stream.context';
import { RedisValue } from 'ioredis';

export class RedisStreamStrategy
  extends Server
  implements CustomTransportStrategy {
  private streamHandlerMap: { [key: string]: any } = {};

  private redis: RedisInstance | null = null;

  private client: RedisInstance | null = null;

  constructor(private readonly options: ConstructorOptions) {
    super();
  }

  // Implement abstract methods required by NestJS Server in v11+
  public on<EventKey extends keyof Record<string, Function> = keyof Record<string, Function>, EventCallback extends Record<string, Function>[EventKey] = Record<string, Function>[EventKey]>(
    event: EventKey,
    callback: EventCallback,
  ): any {
    // Delegate to the underlying redis instance if available
    if (this.redis && typeof (this.redis as any).on === 'function') {
      (this.redis as any).on(event as any, callback as any);
    }
    return undefined;
  }

  public unwrap<T>(): T {
    // Expose the underlying server/broker instance (redis)
    return (this.redis as unknown) as T;
  }

  public listen(callback: () => void) {
    this.redis = createRedisConnection(this.options?.connection);
    this.client = createRedisConnection(this.options?.connection);

    // register instances for error handling.
    this.handleError(this.redis);
    this.handleError(this.client);

    // when server instance connect, bind handlers.
    this.redis.on(RedisEventsMap.CONNECT, () => {
      this.logger.log(
        'Redis connected successfully on ' +
        (this.options.connection?.url ??
          this.options.connection?.host +
          ':' +
          this.options.connection?.port),
      );

      this.bindHandlers();

      // Essential. or await app.listen() will hang forever.
      // Any code after it won't work.
      callback();
    });
  }

  public async bindHandlers() {
    try {
      // collect handlers from user-land, and register the streams.
      await Promise.all(
        Array.from(this.messageHandlers.keys()).map(async (pattern: string) => {
          await this.registerStream(pattern);
        }),
      );

      this.listenOnStreams();
    } catch (error) {
      this.logger.error(error);
      throw error;
    }
  }

  private async registerStream(pattern: string) {
    try {
      this.streamHandlerMap[pattern] = this.messageHandlers.get(pattern);

      await this.createConsumerGroup(
        pattern,
        this.options?.streams?.consumerGroup,
      );

      return true;
    } catch (error) {
      // JSON.parse will throw error, if is not parsable.
      this.logger.debug!(error + '. Handler Pattern is: ' + pattern);
      return false;
    }
  }

  private async createConsumerGroup(stream: string, consumerGroup: string) {
    try {
      if (!this.redis) throw new Error('Redis instance not found.');

      const modifiedStreamKey = this.prependPrefix(stream);

      await this.redis.xgroup(
        'CREATE',
        modifiedStreamKey,
        consumerGroup,
        '$',
        'MKSTREAM',
      );

      return true;
    } catch (error) {
      // if group exist for this stream. log debug.
      if (error instanceof Error && error.message.includes('BUSYGROUP')) {
        this.logger.debug!(
          'Consumer Group "' +
          consumerGroup +
          '" already exists for stream: ' +
          this.prependPrefix(stream),
        );
        return true;
      } else {
        this.logger.error(error);
        return false;
      }
    }
  }

  private async publishResponses(
    responses: StreamResponseObject[],
    inboundContext: RedisStreamContext,
  ) {
    try {
      await Promise.all(
        responses.map(async (responseObj: StreamResponseObject) => {
          let serializedEntries: string[];

          // if custom serializer is provided.
          if (typeof this.options?.serialization?.serializer === 'function') {
            serializedEntries = await this.options.serialization.serializer(
              responseObj?.payload,
              inboundContext,
            );
          } else {
            serializedEntries = await serialize(
              responseObj?.payload,
              inboundContext,
            );
          }

          if (!this.client) throw new Error('Redis client instance not found.');

          const commandArgs: RedisValue[] = [];
          if (this.options.streams?.maxLen) {
            commandArgs.push("MAXLEN")
            commandArgs.push("~")
            commandArgs.push(this.options.streams.maxLen.toString())
          }
          commandArgs.push("*")

          await this.client.xadd(
            responseObj.stream,
            ...commandArgs,
            ...serializedEntries,
          );
        }),
      );

      return true;
    } catch (error) {
      this.logger.error(error);
      return false;
    }
  }

  private async handleAck(inboundContext: RedisStreamContext) {
    try {
      if (!this.client) throw new Error('Redis client instance not found.');

      await this.client.xack(
        inboundContext.getStream(),
        inboundContext.getConsumerGroup(),
        inboundContext.getMessageId(),
      );

      if (true === this.options?.streams?.deleteMessagesAfterAck) {
        await this.client.xdel(
          inboundContext.getStream(),
          inboundContext.getMessageId(),
        );
      }

      return true;
    } catch (error) {
      this.logger.error(error);
      return false;
    }
  }

  private async handleRespondBack({
    response,
    inboundContext,
    isDisposed,
  }: {
    response: StreamResponse;
    inboundContext: RedisStreamContext;
    isDisposed: boolean;
  }) {
    try {
      // if response is null or undefined, do not ACK, neither publish anything.
      if (!response) return;

      // if response is empty array, only ACK.
      if (Array.isArray(response) && response.length === 0) {
        await this.handleAck(inboundContext);
        return;
      }

      // otherwise, publish response, then Xack.
      if (Array.isArray(response) && response.length >= 1) {
        const publishedResponses = await this.publishResponses(
          response,
          inboundContext,
        );

        if (!publishedResponses) {
          throw new Error('Could not Xadd response streams.');
        }

        await this.handleAck(inboundContext);
      }
    } catch (error) {
      this.logger.error(error);
    }
  }

  private async notifyHandlers(stream: string, messages: any[]) {
    try {
      const modifiedStream = this.stripPrefix(stream);
      const handler = this.streamHandlerMap[modifiedStream];

      await Promise.all(
        messages.map(async (message) => {
          const ctx = new RedisStreamContext([
            modifiedStream,
            message[0], // message id needed for ACK.
            this.options?.streams?.consumerGroup,
            this.options?.streams?.consumer,
          ]);

          let parsedPayload: any;

          // if custom deserializer is provided.
          if (typeof this.options?.serialization?.deserializer === 'function') {
            parsedPayload = await this.options.serialization.deserializer(
              message,
              ctx,
            );
          } else {
            parsedPayload = await deserialize(message, ctx);
          }

          // the staging function, should attach the inbound context to keep track of
          //  the message id for ACK, group name, stream name, etc.
          const stageRespondBack = (responseObj: any) => {
            responseObj.inboundContext = ctx;
            this.handleRespondBack(responseObj);
          };

          const response$ = this.transformToObservable(
            await handler(parsedPayload, ctx),
          ) as any;

          // Only call send if we received a valid observable (has pipe),
          // or if send has been overridden on the instance (e.g., in tests).
          const shouldCallSend =
            !!response$ &&
            (typeof response$?.pipe === 'function' ||
              Object.prototype.hasOwnProperty.call(this, 'send'));

          if (shouldCallSend) {
            this.send(response$, stageRespondBack as any);
          }
        }),
      );
    } catch (error) {
      this.logger.error(error);
    }
  }

  private async listenOnStreams(): Promise<void> {
    try {
      if (!this.redis) throw new Error('Redis instance not found.');

      const results: any[] = await this.redis.xreadgroup(
        'GROUP',
        this.options?.streams?.consumerGroup || '',
        this.options?.streams?.consumer || '',
        'BLOCK',
        this.options?.streams?.block || 0,
        'STREAMS',
        ...(Object.keys(this.streamHandlerMap).map((s) =>
          this.stripPrefix(s),
        ) as string[]), // streams keys
        ...(
          Object.keys(this.streamHandlerMap).map((s) =>
            this.stripPrefix(s),
          ) as string[]
        ).map((stream: string) => '>'), // '>', this is needed for xreadgroup as id.
      );

      // if BLOCK time ended, and results are null, listen again.
      if (!results) return this.listenOnStreams();

      for (const result of results) {
        const [stream, messages] = result;
        await this.notifyHandlers(stream, messages);
      }

      return this.listenOnStreams();
    } catch (error) {
      console.log('Error in listenOnStreams: ', error);
      this.logger.error(error);
    }
  }

  // When the stream handler name is stored in streamHandlerMap, its stored WITH the key prefix, so sending additional redis commands when using the prefix with the existing key will cause a duplicate prefix. This ensures to strip the first occurrence of the prefix when binding listeners.
  private stripPrefix(streamHandlerName: string) {
    const keyPrefix = this?.redis?.options?.keyPrefix;
    if (!keyPrefix || !streamHandlerName.startsWith(keyPrefix)) {
      return streamHandlerName;
    }
    // Replace just the first instance of the substring
    return streamHandlerName.replace(keyPrefix, '');
  }

  // xgroup CREATE command with ioredis does not automatically prefix the keyPrefix, though many other commands do, such as xreadgroup.
  // https://github.com/redis/ioredis/issues/1659
  private prependPrefix(key: string) {
    const keyPrefix = this?.redis?.options?.keyPrefix;
    if (keyPrefix && !key.startsWith(keyPrefix)) {
      return `${keyPrefix}${key}`;
    } else {
      return key;
    }
  }

  // for redis instances. need to add mechanism to try to connect back.
  public handleError(stream: any) {
    if (!stream || typeof stream.on !== 'function') {
      return;
    }
    stream.on(RedisEventsMap.ERROR, (err: any) => {
      this.logger.error('Redis instance error: ' + err);
      this.close();
    });
  }

  public close() {
    // shut down instances.
    this.redis && this.redis.quit();
    this.client && this.client.quit();
  }
}
