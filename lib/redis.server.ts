import { Server, CustomTransportStrategy, WritePacket } from '@nestjs/microservices';
import { RedisEventsMap } from '@nestjs/microservices/events/redis.events';
import {
  ConstructorOptions,
  RedisInstance,
  StreamResponse,
} from './interfaces';
import { createRedisConnection } from './redis.utils';
import { deserialize, serialize } from './streams.utils';
import { RedisStreamContext } from './stream.context';
import { RedisValue } from 'ioredis';
import { Logger } from '@nestjs/common';

export class RedisStreamStrategy
  extends Server
  implements CustomTransportStrategy {
  // Exposed for tests
  public streamHandlerMap: { [key: string]: any } = {};

  logger = new Logger(RedisStreamStrategy.name);
  public redis: RedisInstance | null = null;
  public client: RedisInstance | null = null;

  private isShuttingDown = false;
  private activeJobs = 0;
  private shutdownPromise: Promise<void> | null = null;
  private shutdownResolve: (() => void) | null = null;
  private signalHandlers = new Map<NodeJS.Signals, (...args: any[]) => void>();

  constructor(public readonly options: ConstructorOptions) {
    super();
  }

  // Implement abstract method required by NestJS Server in v11+
  public on(): any {
    throw new Error('Method not implemented.');
  }

  public unwrap<T>(): T {
    return (this.redis as unknown) as T;
  }

  public listen(callback: () => void) {
    this.redis = createRedisConnection(this.options?.connection);
    this.client = createRedisConnection(this.options?.connection);

    // register instances for error handling.
    this.handleError(this.redis);
    this.handleError(this.client);

    // when server instance connects, bind handlers.
    if (this.redis && typeof (this.redis as any).on === 'function') {
      this.redis.on(RedisEventsMap.CONNECT, () => {
        this.logger.log(
          'Redis connected successfully on ' +
          (this.options.connection?.url ??
            this.options.connection?.host + ':' + this.options.connection?.port),
        );

        // best-effort, any error will be logged in bindHandlers
        void this.bindHandlers();

        // Essential. or await app.listen() will hang forever.
        callback();
      });
    }
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
        this.options?.streams?.consumerGroup as string,
      );

      return true;
    } catch (error) {
      this.logger.debug?.(
        (error as any) + '. Handler Pattern is: ' + pattern,
      );
      return false;
    }
  }

  private async createConsumerGroup(stream: string, consumerGroup: string) {
    try {
      if (!this.redis) throw new Error('Redis instance not found.');

      const streamKey = this.prependPrefix(stream);

      await this.redis.xgroup('CREATE', streamKey, consumerGroup, '$', 'MKSTREAM');

      return true;
    } catch (error) {
      // if group exist for this stream. log debug.
      if (error instanceof Error && error.message.includes('BUSYGROUP')) {
        this.logger.debug?.(
          'Consumer Group "' + consumerGroup + '" already exists for stream: ' + this.prependPrefix(stream),
        );
        return true;
      } else {
        this.logger.error(error);
        return false;
      }
    }
  }

  private async publishResponses(
    response: any,
    stream: string,
    inboundContext: RedisStreamContext,
  ) {
    try {
      let serializedEntries: string[];

      // if custom serializer is provided.
      if (typeof this.options?.serialization?.serializer === 'function') {
        serializedEntries = await this.options.serialization.serializer(
          response,
          inboundContext,
        );
      } else {
        serializedEntries = await serialize(
          { data: response },
          inboundContext,
        );
      }

      if (!this.client) throw new Error('Redis client instance not found.');

      const commandArgs: RedisValue[] = [];
      if (this.options.streams?.maxLen) {
        commandArgs.push('MAXLEN');
        commandArgs.push('~');
        commandArgs.push(this.options.streams.maxLen.toString());
      }
      commandArgs.push('*');

      await this.client.xadd(
        stream,
        ...commandArgs,
        ...serializedEntries,
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
    stream,
    isDisposed,
  }: {
    response: StreamResponse;
    inboundContext: RedisStreamContext;
    stream: string;
    isDisposed?: boolean;
  }) {
    try {
      // if response is null or undefined, just ACK.
      if (!response) {
        await this.handleAck(inboundContext);
        return;
      }

      if (inboundContext.getMessageHeaders()["streamType"] === "send") {
        // otherwise, publish response, then Xack.
        const publishedResponses = await this.publishResponses(
          response,
          stream,
          inboundContext,
        );

        if (!publishedResponses) {
          // Log the error and do not ACK since publishing failed
          this.logger.error(new Error('Could not Xadd response streams.'));
          return;
        }
      }

      await this.handleAck(inboundContext);
    } catch (error) {
      this.logger.error(error);
    } finally {
      if (isDisposed) {
        this.onJobDone();
      }
    }
  }

  private onJobStart() {
    this.activeJobs++;
  }

  private onJobDone() {
    if (this.activeJobs > 0) {
      this.activeJobs--;
    }
    if (this.isShuttingDown && this.activeJobs === 0 && this.shutdownResolve) {
      this.shutdownResolve();
      this.shutdownResolve = null;
      this.shutdownPromise = null;
    }
  }

  private async notifyHandlers(stream: string, messages: any[]) {
    try {
      const modifiedStream = this.stripPrefix(stream);
      const handler = this.streamHandlerMap[modifiedStream];

      await Promise.all(
        messages.map(async (message) => {
          this.onJobStart();

          const ctx = new RedisStreamContext([
            modifiedStream,
            message[0], // message id needed for ACK.
            this.options?.streams?.consumerGroup as string,
            this.options?.streams?.consumer as string,
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

          const stageRespondBack = (packet: WritePacket) => {
            this.handleRespondBack({
              response: packet.response,
              inboundContext: ctx,
              stream: `${stream}:response`,
              isDisposed: packet.isDisposed,
            });
          };

          const response$ = this.transformToObservable(
            await handler(parsedPayload, ctx),
          ) as any;

          const shouldCallSend =
            !!response$ &&
            (typeof response$?.pipe === 'function' ||
              Object.prototype.hasOwnProperty.call(this, 'send'));

          if (shouldCallSend) {
            this.send(response$, stageRespondBack as any);
          } else {
            this.onJobDone();
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
      if (this.isShuttingDown) return;

      const streams = Object.keys(this.streamHandlerMap);
      const results: any[] = await this.redis.xreadgroup(
        'GROUP',
        this.options?.streams?.consumerGroup || '',
        this.options?.streams?.consumer || '',
        'BLOCK',
        this.options?.streams?.block || 0,
        'STREAMS',
        ...streams,
        ...streams.map(() => '>'),
      );

      if (this.isShuttingDown) return;

      // if BLOCK time ended, and results are null, listen again.
      if (!results) return this.listenOnStreams();

      for (const result of results) {
        const [stream, messages] = result;
        await this.notifyHandlers(stream, messages);
      }

      return this.listenOnStreams();
    } catch (error) {
      this.logger.error(error);
    }
  }

  // When the stream handler name is stored in streamHandlerMap, it's stored WITH the key prefix,
  // so sending additional redis commands when using the prefix with the existing key will cause a duplicate prefix.
  // This ensures to strip the first occurrence of the prefix when binding listeners.
  private stripPrefix(streamHandlerName: string) {
    const keyPrefix = this?.redis?.options?.keyPrefix;
    if (!keyPrefix || !streamHandlerName.startsWith(keyPrefix)) {
      return streamHandlerName;
    }
    // Replace just the first instance of the substring
    return streamHandlerName.replace(keyPrefix, '');
  }

  // xgroup CREATE command with ioredis does not automatically prefix the keyPrefix,
  // though many other commands do, such as xreadgroup.
  // https://github.com/redis/ioredis/issues/1659
  private prependPrefix(key: string) {
    const keyPrefix = this?.redis?.options?.keyPrefix;
    if (keyPrefix && !key.startsWith(keyPrefix)) {
      return `${keyPrefix}${key}`;
    } else {
      return key;
    }
  }

  private async deregisterConsumer() {
    try {
      if (!this.client) return;

      const consumerGroup = this.options?.streams?.consumerGroup as string;
      const consumer = this.options?.streams?.consumer as string;

      const streams = Object.keys(this.streamHandlerMap).map((s) =>
        this.prependPrefix(this.stripPrefix(s)),
      );

      await Promise.all(
        streams.map((stream) =>
          this.client!.xgroup('DELCONSUMER', stream, consumerGroup, consumer),
        ),
      );
    } catch (error) {
      this.logger.error(error);
    }
  }

  private cleanupSignalHandlers() {
    for (const [sig, handler] of this.signalHandlers) {
      try {
        process.removeListener(sig, handler);
      } catch {
        // ignore
      }
    }
    this.signalHandlers.clear();
  }

  public async shutdownGracefully() {
    this.logger.log('Shutdown initiated');
    if (this.isShuttingDown) return;
    this.isShuttingDown = true;

    try {

      if (this.options?.shutdown?.deregisterConsumer !== false) {
        this.logger.log('Deregistering consumer group');
        await this.deregisterConsumer();
      }

      const timeout = this.options?.shutdown?.drainTimeoutMs;
      if (this.activeJobs > 0) {
        if (!this.shutdownPromise) {
          this.shutdownPromise = new Promise<void>((resolve) => {
            this.shutdownResolve = resolve;
          });
        }
        if (typeof timeout === 'number' && timeout > 0) {
          await Promise.race([
            this.shutdownPromise,
            new Promise<void>((resolve) => setTimeout(resolve, timeout)),
          ]);
        } else {
          await this.shutdownPromise;
        }
      }
    } finally {
      this.cleanupSignalHandlers();
      // Now actually close connections
      this.redis && this.redis.quit();
      this.client && this.client.quit();

      if (this.redis) {
        try {
          // disconnect to unblock any pending xreadgroup
          this.redis.disconnect();
        } catch {
          // ignore
        }
      }
      if (this.options?.shutdown?.exitProcess) {
        process.exit(0);
      }
    }
  }

  public handleError(stream: any) {
    if (!stream || typeof stream.on !== 'function') {
      return;
    }
    stream.on(RedisEventsMap.ERROR, (err: any) => {
      this.logger.error('Redis instance error: ' + err);
      this.close();
    });
  }

  public async close() {
    if (!this.isShuttingDown) {
      await this.shutdownGracefully();
      return;
    }
  }
}
