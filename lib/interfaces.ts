import { ModuleMetadata, Type } from '@nestjs/common';
import * as Redis from 'ioredis';
import { RedisStreamContext } from './stream.context';

export type RedisConnectionOptions = Redis.RedisOptions & { url?: string };

export type RedisInstance = Redis.Redis;

export interface RedisStreamPattern {
  stream: string;
}

interface RedisStreamOptionsXreadGroup {
  block?: number;
  consumerGroup: string;
  consumer: string;
  deleteMessagesAfterAck?: boolean;
  /**
   * Configure per-stream concurrency (COUNT for XREADGROUP) using regex-like patterns.
   * Example:
   *   perStreamConcurrency: [
   *     { pattern: 'user.*', count: 10 },
   *     { pattern: '^orders:(created|updated)
, count: 5 },
   *   ]
   */
  perStreamConcurrency?: Array<{ pattern: string; count: number }>;
}

interface RedisStreamOptionsXadd {
  maxLen?: number;
}

export type RedisStreamOptions = RedisStreamOptionsXreadGroup & RedisStreamOptionsXadd;

// [id, [key, value, key, value]]
export type RawStreamMessage = [id: string, payload: string[]];

export interface Serialization {
  deserializer?: (
    rawMessage: RawStreamMessage,
    inboundContext: RedisStreamContext,
  ) => any | Promise<any>;

  serializer?: (
    parsedPayload: any,
    inboundContext: RedisStreamContext,
  ) => string[] | Promise<string[]>;
}

export interface GracefulShutdownOptions {
  signals?: NodeJS.Signals[];
  drainTimeoutMs?: number;
  deregisterConsumer?: boolean;
  exitProcess?: boolean;
}

export interface ConstructorOptions {
  connection?: RedisConnectionOptions;
  streams: RedisStreamOptions;
  serialization?: Serialization;
  shutdown?: GracefulShutdownOptions;
}

export interface ClientConstructorOptions extends ConstructorOptions {
  responseStreams?: string[];
}

export interface StreamResponseObject {
  payload: {
    [key: string]: any; // any extra keys goes as headers.
    data: any;
  };
  stream: string;
}

export type StreamResponse = StreamResponseObject[] | [] | null | undefined;

export interface RedisStreamClientModuleOptionsFactory {
  createRedisStreamClientModuleOptions():
    | Promise<ClientConstructorOptions>
    | ClientConstructorOptions;
}

// for the client module .registerAsync() method
export interface RedisStreamModuleAsyncOptions
  extends Pick<ModuleMetadata, 'imports'> {
  useExisting?: Type<RedisStreamClientModuleOptionsFactory>;
  useClass?: Type<RedisStreamClientModuleOptionsFactory>;
  useFactory?: (
    ...args: any[]
  ) => Promise<ClientConstructorOptions> | ClientConstructorOptions;
  inject?: any[];
}
