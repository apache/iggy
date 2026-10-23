// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { EventEmitter } from 'node:events';
import type {
  ClientConfig, ClientConfigOrString, ClientCredentials, CommandResponse,
  PasswordCredentials, RawClient, SendCommandOptions, TokenCredentials
} from './client.type.js';
import { DeserializeError, ResponseError, responseError } from '../wire/error.utils.js';
import { IggyConnection, endpointKey, type Endpoint } from './client.connection.js';
import { normalizeClientConfig } from './client.config.js';
import {
  ChangeSignal, MutationLanes, NotConnectedError, OperationUncertainError, OperationNotRetainedError,
  OPERATION_TIMEOUT_MS, VsrResponseTimeoutError, pause, waitWithin, type ClientOperation
} from './client.operation.js';
import { LOGIN, LOGIN_WITH_TOKEN, LOGOUT, PING } from '../wire/index.js';
import { GET_CLUSTER_METADATA } from '../wire/cluster/get-cluster-metadata.command.js';
import { deserializePartitionRoute, type PartitionRoute } from '../wire/partition/partition-route.js';
import { ConsumerKind } from '../wire/offset/offset.utils.js';
import { COMMAND_CODE } from '../wire/command.code.js';
import { serializeIdentifier } from '../wire/identifier.utils.js';
import { deserializeOptions } from '../wire/options.utils.js';
import { OptionsScope } from '../wire/system/describe-options.command.js';
import { PartitionKind } from '../wire/message/partitioning.utils.js';
import { PollingStrategyKind } from '../wire/message/poll.utils.js';
import {
  EvictionReason, HEADER_SIZE, REPLY_OFFSET, hasCommittedResult, type PartitionContext,
  EMPTY_PARTITION_CONTEXT, readReplyOperation
} from '../wire/vsr/header.js';
import { Operation, isMetadata, operationForCode } from '../wire/vsr/operation.js';
import {
  decodeVsrResponse, prepareVsrCommand, readRegisteredSession,
  readWireName, VsrEvictionError, VsrSession
} from '../wire/vsr/index.js';
import { validateReplyForRequest } from '../wire/vsr/reply.js';
import { debug } from './client.debug.js';

export {
  LaneBlockedError, NotConnectedError, OperationNotRetainedError, OperationUncertainError,
  VsrResponseTimeoutError
} from './client.operation.js';

const RETRY_INTERVAL_MS = 50;
const MAX_RETRY_INTERVAL_MS = 1_000;
const MAX_LIFECYCLE_INTERVAL_MS = 1_000;
const MAX_RECONNECT_INTERVAL_MS = 10_000;
const MAX_REPLICAS = 256;
const MAX_CONCURRENT_DIALS = 4;
const MAX_PARTITION_ROUTES = 4096;
const TRANSIENT_NOT_COMMITTED = 57;
const TRANSIENT_NOT_ACCEPTED = 58;
const UNAUTHENTICATED = 40;
const STALE_CLIENT = 30;
const OPERATION_NOT_RETAINED = 85;
const HISTORY_UNAVAILABLE = 87;
const LIFECYCLE_BUSY = 88;
const NOT_PRIMARY = 89;
const INVALID_COMMAND = 3;
const FEATURE_UNAVAILABLE = 5;
const INVALID_SESSION_BINDING = 14004;
const ROUTE_NAMESPACE_MISSING = new Set([1009, 1010, 2010, 2011, 3007]);
const POLL_OPTIONS_SIZE = 9 + 4 + 1;
const IDENTIFIER_PREFIX_SIZE = 2;
const SEND_METADATA_LENGTH_SIZE = 4;
const PARTITION_ID_SIZE = 4;
const STORE_OFFSET_SUFFIX_SIZE = 8 + 1;
const DELETE_OFFSET_SUFFIX_SIZE = 1;
const RAW_SEND_NEEDS_PARTITION =
  'Raw sends need an explicit partition id. Use the typed send API for Balanced or MessageKey partitioning.';

const TOPIC_DISCOVERY_RESETS = new Set<number>([
  COMMAND_CODE.DeleteStream, COMMAND_CODE.DeleteTopic,
  COMMAND_CODE.CreatePartitions, COMMAND_CODE.DeletePartitions
]);
const READ_COMMANDS = new Set<number>([
  COMMAND_CODE.Ping, COMMAND_CODE.GetStats, COMMAND_CODE.GetClusterMetadata,
  COMMAND_CODE.DescribeOptions, COMMAND_CODE.GetMe, COMMAND_CODE.GetClient,
  COMMAND_CODE.GetClients, COMMAND_CODE.GetUser, COMMAND_CODE.GetUsers,
  COMMAND_CODE.GetAccessTokens, COMMAND_CODE.GetPartitionRoute, COMMAND_CODE.GetOffset,
  COMMAND_CODE.GetStream, COMMAND_CODE.GetStreams, COMMAND_CODE.GetTopic,
  COMMAND_CODE.GetTopics, COMMAND_CODE.GetGroup, COMMAND_CODE.GetGroups, COMMAND_CODE.SyncGroup
]);

type ReplicaSlot = {
  readonly replica: number,
  /** The advertised endpoint, which later rosters compare against. */
  readonly endpoint: Endpoint,
  readonly dialEndpoint: Endpoint,
  readonly generation: number,
  readonly lifetime: AbortController,
  connection?: IggyConnection,
  attached: boolean,
  initial: boolean,
  heartbeat: boolean,
  restartable: boolean,
  failure?: unknown
};
type CachedRoute = {
  route: PartitionRoute,
  group: string,
  minimumView: number,
  stale: boolean
};
type Attempt = {
  response?: CommandResponse,
  raw?: Buffer,
  error?: unknown,
  committed: boolean,
  started: boolean
};

/** A replica link used its retry budget, and nothing restarts it. */
class ReconnectExhaustedError extends Error {
  constructor() {
    super('replica reconnect attempts exhausted');
  }
}

/** One logical session owns every link, mutation lane and retained operation. */
export class CommandResponseStream extends EventEmitter {
  private readonly options: ClientConfig;
  private readonly vsrSession = new VsrSession();
  private readonly lanes: MutationLanes;
  private readonly replicas = new Map<number, ReplicaSlot>();
  private readonly partitionRoutes = new Map<string, CachedRoute>();
  private readonly changed = new ChangeSignal();
  private readonly lifetime = new AbortController();
  private sessionLifetime = new AbortController();
  private bootstrap?: IggyConnection;
  private bootstrapPromise?: Promise<IggyConnection>;
  /**
   * What the running bootstrap reports when it gives up: the bind failure of
   * the last endpoint that accepted a dial, else NotConnectedError with the
   * last dial failure.
   */
  private bootstrapFailure?: unknown;
  private authenticationPromise?: Promise<boolean>;
  private sessionAction: Promise<unknown> = Promise.resolve();
  private recoveryPromise?: Promise<void>;
  private rosterPromise?: Promise<void>;
  /**
   * Whom the client signs in as by itself: the caller's last sign-in, else the
   * configured credentials while the caller never signed in. A logout, a
   * sign-in of the caller that fails and a refused sign-in clear it.
   */
  private signInCredentials?: ClientCredentials;
  /**
   * Raised by each sign-in and sign-out of the caller. A call continues under
   * a new session only while the lineage it started with is unchanged.
   */
  private sessionLineage = 0;
  private knownEndpoints: Endpoint[] = [];
  private readonly configuredEndpoint: Endpoint;
  private metadataReplica?: number;
  private metadataWatermark = 0n;
  private routingGeneration = 0;
  private activeDials = 0;
  private pendingSubmissions = 0;
  private heartbeatIntervalHandler?: NodeJS.Timeout;
  isAuthenticated = false;
  userId?: number;

  constructor(config: ClientConfigOrString) {
    super();
    this.options = normalizeClientConfig(config);
    this.signInCredentials = this.options.credentials;
    this.configuredEndpoint = { host: this.options.options.host ?? '127.0.0.1', port: this.options.options.port };
    this.lanes = new MutationLanes(this.options.maxPendingOperations!, this.options.maxPendingOperationBytes!);
  }

  get lineage(): number { return this.sessionLineage; }

  get busy(): boolean { return this.pendingSubmissions > 0; }

  async sendCommand(command: number, payload: Buffer, options: SendCommandOptions = {}): Promise<CommandResponse> {
    if (isLoginCommand(command)) return this.loginByCaller(command, () => Buffer.from(payload), options);
    const lineage = options.lineage ?? this.lineage;
    payload = Buffer.from(payload);
    const release = this.hold();
    const deadline = options.deadline ?? Date.now() + OPERATION_TIMEOUT_MS;
    const signal = AbortSignal.any([this.lifetime.signal, ...(options.signal ? [options.signal] : [])]);
    try {
      if (command === LOGOUT.code) {
        const signedOut = ++this.sessionLineage;
        this.signInCredentials = undefined;
        return await this.serializeSession(async () => {
          this.ensureLineage(signedOut, command);
          try {
            if (!this.isAuthenticated)
              throw responseError(command, UNAUTHENTICATED);
            return await this.drive(command, payload, options, deadline, signal, signedOut);
          } finally {
            if (signedOut === this.lineage) {
              this.signInCredentials = undefined;
              this.resetSession();
            }
          }
        });
      }
      if (lineage !== this.lineage) throw responseError(command, UNAUTHENTICATED);
      if (!this.isAuthenticated && command !== PING.code) {
        void this.signIn();
        await this.nextSession(command, lineage, deadline, signal);
      }
      if (this.isAuthenticated && this.replicas.size === 0) {
        const generation = this.routingGeneration;
        // The call waits for a session lost meanwhile itself.
        await this.refreshRoster(deadline, signal).catch((error: unknown) => {
          if (generation !== this.routingGeneration) return;
          // The call sent nothing yet, so a deadline leaves it not accepted.
          throw error instanceof VsrResponseTimeoutError ? responseError(command, TRANSIENT_NOT_ACCEPTED) : error;
        });
      }
      return await this.drive(command, payload, options, deadline, signal, lineage);
    } finally {
      release();
    }
  }

  async resolveOperation(operationId: bigint, options: SendCommandOptions = {}): Promise<void> {
    try {
      await this.replay(this.lanes.retained(operationId), options);
    } catch (error) {
      // A replay that proved the outcome released the operation.
      if (!(error instanceof OperationNotRetainedError)) throw error;
      this.lanes.provedOutcome(operationId);
    }
  }

  abandonOperation(operationId: bigint): void {
    this.lanes.abandon(operationId);
  }

  private async replay(operation: ClientOperation, options: SendCommandOptions): Promise<void> {
    const lineage = options.lineage ?? this.lineage;
    operation.frame.retain();
    const release = this.hold();
    const deadline = options.deadline ?? Date.now() + OPERATION_TIMEOUT_MS;
    const signal = AbortSignal.any([this.lifetime.signal, this.sessionLifetime.signal,
      operation.lifetime.signal, ...(options.signal ? [options.signal] : [])]);
    let unlock: (() => void) | undefined;
    try {
      unlock = await this.lanes.acquireResolution(operation, deadline, signal);
      await this.drive(operation.command, operation.frame.buffer.subarray(HEADER_SIZE),
        { context: operation.context }, deadline, signal, lineage, operation);
    } finally {
      unlock?.();
      operation.frame.release();
      release();
    }
  }

  private async drive(command: number, body: Buffer, options: SendCommandOptions,
    deadline: number, signal: AbortSignal, lineage: number, retained?: ClientOperation): Promise<CommandResponse> {
    /** The session that the frame of the call belongs to. */
    let generation = this.routingGeneration;
    const target = partitionRoute(command, body);
    const replicated = command !== LOGOUT.code && operationForCode(command) !== Operation.NonReplicated;
    const progress = command === COMMAND_CODE.PollMessages &&
      (body[body.length - 1] !== 0 || body[body.length - POLL_OPTIONS_SIZE] === PollingStrategyKind.Next);
    const safeRead = READ_COMMANDS.has(command) || (command === COMMAND_CODE.PollMessages && !progress);
    // A replay and a logout belong to their session. Any other call that
    // needs a session waits for the one that replaces a lost one.
    const restartable = !retained && command !== LOGOUT.code && (command !== PING.code || this.isAuthenticated);
    let operation = retained;
    let frameLease = false;
    /** The frame of the operation belongs to a replaced session, or an 88 spent its request id. */
    let staleFrame = false;
    let frame: Buffer | undefined;
    let context = options.context ? { ...options.context } : undefined;
    let selected: CachedRoute | undefined;
    let historyRefreshed = false;
    let lifecycleDelay = RETRY_INTERVAL_MS;
    let retryDelay = RETRY_INTERVAL_MS;
    let notPrimary = false;
    let refresh = false;
    let lastRefusal: ResponseError | undefined;
    let previousLink: IggyConnection | undefined;
    let sent = false;
    try {
      for (;;) {
        this.ensureLineage(lineage, command);
        signal.throwIfAborted();
        if (Date.now() >= deadline)
          throw new VsrResponseTimeoutError(0);
        if (generation !== this.routingGeneration || (restartable && !this.isAuthenticated)) {
          // No session but the one of a frame can apply it. A frame that can
          // have taken effect ends uncertain and never goes out again.
          if (operation?.uncertain)
            throw new OperationUncertainError(operation.id, responseError(command, TRANSIENT_NOT_ACCEPTED));
          if (!restartable)
            throw responseError(command, TRANSIENT_NOT_ACCEPTED);
          await this.nextSession(command, lineage, deadline, signal);
          generation = this.routingGeneration;
          selected = undefined;
          previousLink = undefined;
          refresh = false;
          frame = undefined;
          // A frame of the new session carries the context of the route that
          // the new session looks up.
          context = options.context ? { ...options.context } : undefined;
          if (operation) {
            if (frameLease) operation.frame.release();
            frameLease = false;
            staleFrame = true;
          }
        }
        const sessionSignal = AbortSignal.any([signal, this.sessionLifetime.signal]);
        if (target && !selected) {
          try {
            selected = await this.routeFor(target, deadline, sessionSignal, generation);
          } catch (error) {
            // A session failure answered the lookup of the call itself.
            if (restartable && generation !== this.routingGeneration && !isSessionFailure(error)) continue;
            throw error;
          }
          context ??= { ...selected.route.context };
        }
        context ??= { ...EMPTY_PARTITION_CONTEXT, metadataOp: this.metadataWatermark };
        this.ensureLineage(lineage, command);
        const lane = target ? groupKey(context, target) : 'metadata';
        if (operation && staleFrame && operation.lane !== lane) {
          // A definitive refusal released the old history. The replacement
          // must enter the lane that orders writes to its current incarnation.
          this.lanes.complete(operation);
          operation = undefined;
        }
        if (operation && staleFrame) {
          staleFrame = false;
          try {
            await this.lanes.restart(operation, body, context, this.vsrSession, deadline, signal);
          } catch (error) {
            if (generation === this.routingGeneration) throw error;
            this.lanes.complete(operation);
            operation = undefined;
            continue;
          }
        } else if (!operation && (replicated || progress)) {
          try {
            operation = await this.lanes.enter(lane, command, body, context, replicated,
              this.vsrSession, deadline, signal);
            staleFrame = false;
          } catch (error) {
            // A lane can free while no session is bound to encode the frame.
            if (restartable && generation !== this.routingGeneration) continue;
            throw error;
          }
        }
        if (operation && !frameLease) {
          operation.frame.retain();
          frameLease = true;
        }
        const attemptSignal = operation
          ? AbortSignal.any([sessionSignal, operation.lifetime.signal]) : sessionSignal;
        let attempt: Attempt;
        let exchanged = false;
        try {
          if (refresh && target)
            selected = await this.routeFor(target, deadline, attemptSignal, generation, true);
          else if (refresh)
            await this.refreshRoster(deadline, attemptSignal);
          refresh = false;
          const link = target
            ? await this.primaryLink(selected!, deadline, attemptSignal)
            : await this.readyLink(deadline, attemptSignal, safeRead ? previousLink : undefined);
          previousLink = link;
          this.ensureGeneration(generation, command);
          attempt = await this.exchange(link, command, () => {
            this.ensureGeneration(generation, command);
            this.ensureLineage(lineage, command);
            return operation?.frame.buffer ?? (frame ??= this.vsrSession.encode(command, body, context));
          }, deadline, attemptSignal, operation);
          exchanged = true;
        } catch (error) {
          attempt = { error, committed: false, started: false };
        }
        sent ||= attempt.started;
        if (generation !== this.routingGeneration && attempt.started && (replicated || progress))
          throw uncertainOutcome(command, operation, attempt.error ?? responseError(command, TRANSIENT_NOT_ACCEPTED));
        // A committed rejection proves the outcome even when it also ends the session.
        if (operation?.uncertain && attempt.committed && attempt.error) {
          operation.uncertain = false;
          this.lanes.prove(operation, attempt.error);
          if (isSessionFailure(attempt.error)) this.recoverSession(generation);
          throw attempt.error;
        }
        if (isSessionFailure(attempt.error)) {
          // An eviction does not say whether a written request was applied,
          // while a 40 refusal or a committed 30 answers this exact request.
          const uncertain = operation?.uncertain ||
            (attempt.started && (replicated || progress) && attempt.error instanceof VsrEvictionError);
          this.recoverSession(generation);
          if (uncertain)
            throw uncertainOutcome(command, operation, attempt.error);
          // An eviction fails every exchange of its link, also one never written.
          if (restartable && exchanged && !attempt.started && generation !== this.routingGeneration) continue;
          throw attempt.error;
        }
        if (generation !== this.routingGeneration) {
          if (restartable && (!attempt.started || safeRead)) continue;
          throw responseError(command, TRANSIENT_NOT_ACCEPTED);
        }
        if (!attempt.error) {
          if (operation?.uncertain && !attempt.committed)
            throw new OperationUncertainError(operation.id, new Error('reply does not prove the retained result'));
          if (command === COMMAND_CODE.GetPartitionRoute &&
              !this.rememberRoute({ query: body, key: body.toString('hex') }, attempt.response!)) {
            await waitWithin(pause(Math.floor(Math.random() * (retryDelay + 1)), signal), deadline, signal);
            retryDelay = Math.min(retryDelay * 2, MAX_RETRY_INTERVAL_MS);
            continue;
          }
          if (lineage === this.lineage) this._rememberCredentials(command, body);
          if (command === GET_CLUSTER_METADATA.code)
            this.installRoster(attempt.response!);
          if (TOPIC_DISCOVERY_RESETS.has(command)) {
            for (const route of this.partitionRoutes.values()) route.stale = true;
            this.emit('topicDiscoveryReset');
          }
          if (operation) this.lanes.prove(operation);
          return options.handleResponse === false ? attempt.raw as unknown as CommandResponse : attempt.response!;
        }
        const error = attempt.error;
        if (error instanceof ResponseError && error.fromServer) lastRefusal = error;
        const code = error instanceof ResponseError && error.fromServer ? error.errorCode : undefined;
        const repeatedNotPrimary = notPrimary;
        notPrimary = code === NOT_PRIMARY;
        if (code === TRANSIENT_NOT_COMMITTED) {
          if (operation?.retainable) operation.uncertain = true;
          else if (!safeRead) throw error;
        } else if (code === OPERATION_NOT_RETAINED) {
          if (operation?.retainable) operation.uncertain = true;
          throw uncertainOutcome(command, operation, error);
        } else if (code === NOT_PRIMARY || code === TRANSIENT_NOT_ACCEPTED) {
          if (selected) this.invalidateRoute(selected, code === NOT_PRIMARY);
          refresh = true;
          if (code === NOT_PRIMARY) {
            // Neither 58 nor 89 admitted the call, so the deadline reports both as 58.
            lastRefusal = responseError(command, TRANSIENT_NOT_ACCEPTED);
            lastRefusal.fromServer = true;
          }
          if (code === NOT_PRIMARY && !repeatedNotPrimary) continue;
        } else if (code === HISTORY_UNAVAILABLE && !operation?.uncertain) {
          if (selected) this.invalidateRoute(selected, false);
          if (target && command === COMMAND_CODE.SendMessages && !options.context && !historyRefreshed) {
            historyRefreshed = true;
            if (operation) {
              this.lanes.complete(operation);
              operation.frame.release();
              frameLease = false;
              operation = undefined;
            }
            selected = undefined;
            context = undefined;
            continue;
          }
          throw error;
        } else if (code === LIFECYCLE_BUSY && !operation?.uncertain) {
          // The refusal spent the request id, so the call goes out again as a
          // new operation in its lane, with the context of its route by then.
          if (operation) {
            operation.frame.release();
            frameLease = false;
            staleFrame = true;
          }
          frame = undefined;
          selected = undefined;
          context = options.context ? { ...options.context } : undefined;
          await waitWithin(pause(lifecycleDelay, signal), deadline, signal);
          lifecycleDelay = Math.min(lifecycleDelay * 2, MAX_LIFECYCLE_INTERVAL_MS);
          continue;
        } else if (code !== undefined && ROUTE_NAMESPACE_MISSING.has(code) && attempt.started &&
            (selected || command === COMMAND_CODE.GetPartitionRoute) &&
            !(await this.isMetadataPrimary(previousLink!, generation, deadline, attemptSignal))) {
          // A backup can lag the metadata create that the metadata primary
          // committed. A partition call retries with a new route, and a route
          // lookup asks the next replica. A miss of the lookup that routes a
          // partition call came before the request started and stays final.
          if (selected) {
            this.invalidateRoute(selected, false);
            refresh = true;
          }
          lastRefusal = responseError(command, TRANSIENT_NOT_ACCEPTED);
          lastRefusal.fromServer = true;
        } else if (code !== undefined) {
          if (operation?.uncertain)
            throw new OperationUncertainError(operation.id, error);
          throw error;
        } else {
          if (attempt.started) {
            if (operation?.retainable) operation.uncertain = true;
            else if (!safeRead) throw uncertainOutcome(command, operation, error);
          }
          // NotConnectedError and an exhausted link leave no link up or
          // redialing, so the call fails now instead of at its deadline.
          if (error instanceof TypeError || error instanceof ResponseError || error instanceof VsrEvictionError ||
              signal.aborted || error instanceof NotConnectedError || error instanceof ReconnectExhaustedError)
            throw error;
          refresh = true;
        }
        await waitWithin(pause(Math.floor(Math.random() * (retryDelay + 1)), signal), deadline, signal);
        retryDelay = Math.min(retryDelay * 2, MAX_RETRY_INTERVAL_MS);
      }
    } catch (error) {
      // A call that sent nothing and saw no refusal was not accepted either.
      if (error instanceof VsrResponseTimeoutError && (lastRefusal || !sent))
        error = lastRefusal ?? responseError(command, TRANSIENT_NOT_ACCEPTED);
      if (operation?.uncertain) {
        // A frame of a replaced session must not block the lane of the new one.
        if (generation === this.routingGeneration && this.lanes.owns(operation)) this.lanes.retain(operation);
        throw error instanceof OperationUncertainError ? error : new OperationUncertainError(operation.id, error);
      }
      if (retained && !this.lanes.owns(retained))
        throw error instanceof ResponseError ? error : new OperationNotRetainedError(retained.id);
      if (error instanceof ResponseError && !(error instanceof VsrEvictionError) && error.commandCode !== command) {
        const remapped = responseError(command, error.errorCode);
        remapped.fromServer = error.fromServer;
        remapped.committed = error.committed;
        throw remapped;
      }
      throw error;
    } finally {
      if (operation && !operation.retained) this.lanes.complete(operation);
      if (operation && frameLease) operation.frame.release();
    }
  }

  private async exchange(link: IggyConnection, command: number, frame: () => Buffer,
    deadline: number, signal: AbortSignal, operation?: ClientOperation): Promise<Attempt> {
    const generation = this.routingGeneration;
    let request: Buffer | undefined;
    const attempt = await link.exchange(() => request = frame(), deadline, signal, operation?.frame);
    if (attempt.error || !attempt.response) {
      const error = this.isAuthenticated && command !== COMMAND_CODE.BindSession &&
        isDetachedFailure(attempt.error)
        ? new Error('replica link detached from its session', { cause: attempt.error }) : attempt.error;
      return { error, committed: false, started: attempt.started };
    }
    try {
      validateReplyForRequest(attempt.response, request!, command);
      const response = decodeVsrResponse(attempt.response, command);
      if (generation === this.routingGeneration) this.observeMetadata(attempt.response);
      return { response, raw: attempt.response, committed: hasCommittedResult(attempt.response), started: attempt.started };
    } catch (error) {
      if (this.isAuthenticated && command !== COMMAND_CODE.BindSession && isDetachedFailure(error) &&
          error instanceof ResponseError && error.errorCode === UNAUTHENTICATED) {
        link._destroy();
        const refusal = responseError(command, TRANSIENT_NOT_ACCEPTED);
        refusal.fromServer = true;
        return { error: refusal, committed: false, started: attempt.started };
      }
      if (error instanceof ResponseError && error.committed)
        if (generation === this.routingGeneration) this.observeMetadata(attempt.response);
      return { error, committed: error instanceof ResponseError && error.committed, started: attempt.started };
    }
  }

  private async control(link: IggyConnection, command: number, body: Buffer,
    deadline: number, signal: AbortSignal): Promise<CommandResponse> {
    let frame: Buffer | undefined;
    let retryDelay = RETRY_INTERVAL_MS;
    for (;;) {
      const attempt = await this.exchange(link, command, () => frame ??= this.vsrSession.encode(command, body,
        { ...EMPTY_PARTITION_CONTEXT, metadataOp: this.metadataWatermark }), deadline, signal);
      if (!attempt.error) return attempt.response!;
      if (!(attempt.error instanceof ResponseError) || !attempt.error.fromServer ||
          !isTransient(attempt.error.errorCode))
        throw attempt.error;
      try { await waitWithin(pause(Math.floor(Math.random() * (retryDelay + 1)), signal), deadline, signal); }
      catch (error) { throw error instanceof VsrResponseTimeoutError ? attempt.error : error; }
      retryDelay = Math.min(retryDelay * 2, MAX_RETRY_INTERVAL_MS);
      if (command === GET_CLUSTER_METADATA.code) {
        const alternative = [...this.replicas.values()].find((slot) =>
          slot.attached && slot.connection?.connected && slot.connection !== link);
        link = alternative?.connection ?? link;
      }
    }
  }

  private async routeFor(target: PartitionTarget, deadline: number, signal: AbortSignal,
    generation: number, force = false): Promise<CachedRoute> {
    const cached = this.partitionRoutes.get(target.key);
    if (!force && cached && !cached.stale && cached.route.context.metadataOp >= this.metadataWatermark)
      return cached;
    let lastRefusal: ResponseError | undefined;
    let refreshedRoster = false;
    let previous: IggyConnection | undefined;
    let retryDelay = RETRY_INTERVAL_MS;
    const visited = new Set<IggyConnection>();
    for (;;) {
      this.ensureGeneration(generation, COMMAND_CODE.GetPartitionRoute);
      const link = await this.readyLink(deadline, signal, previous);
      if (visited.has(link)) {
        visited.clear();
        // Each pass starts at the metadata primary, which a roster read after
        // a namespace miss can have moved.
        previous = undefined;
        try { await waitWithin(pause(Math.floor(Math.random() * (retryDelay + 1)), signal), deadline, signal); }
        catch (error) { throw error instanceof VsrResponseTimeoutError && lastRefusal ? lastRefusal : error; }
        retryDelay = Math.min(retryDelay * 2, MAX_RETRY_INTERVAL_MS);
        continue;
      }
      visited.add(link);
      previous = link;
      try {
        const attempt = await this.exchange(link, COMMAND_CODE.GetPartitionRoute,
          () => this.vsrSession.encode(COMMAND_CODE.GetPartitionRoute, target.query,
            { ...EMPTY_PARTITION_CONTEXT, metadataOp: this.metadataWatermark }), deadline, signal);
        if (attempt.error) throw attempt.error;
        this.ensureGeneration(generation, COMMAND_CODE.GetPartitionRoute);
        const entry = this.rememberRoute(target, attempt.response!);
        if (entry) {
          if (!this.replicas.has(entry.route.primaryReplica) && !refreshedRoster) {
            refreshedRoster = true;
            await this.refreshRoster(deadline, signal);
          }
          if (this.replicas.has(entry.route.primaryReplica)) return entry;
          const error = responseError(COMMAND_CODE.GetPartitionRoute, TRANSIENT_NOT_ACCEPTED);
          error.fromServer = true;
          throw error;
        }
      } catch (error) {
        if (isSessionFailure(error)) {
          this.recoverSession(generation);
          throw error;
        }
        this.ensureGeneration(generation, COMMAND_CODE.GetPartitionRoute);
        if (error instanceof ResponseError && ROUTE_NAMESPACE_MISSING.has(error.errorCode) &&
            !(await this.isMetadataPrimary(link, generation, deadline, signal))) {
          // A backup can lag a metadata create while the primary materializes the route.
          lastRefusal = responseError(COMMAND_CODE.GetPartitionRoute, TRANSIENT_NOT_ACCEPTED);
          lastRefusal.fromServer = error.fromServer;
          continue;
        }
        if (error instanceof DeserializeError || (error instanceof ResponseError && !isTransient(error.errorCode))) throw error;
        if (error instanceof ResponseError) lastRefusal = error;
      }
    }
  }

  /**
   * Whether the replica that answered a namespace miss on `link` is the
   * metadata primary. The belief can predate a view change, so one roster read
   * on `link`, without a retry or another link, must still name that replica.
   * No lookup reaches a believed metadata primary without a link, so then that
   * roster also moves the belief.
   */
  private async isMetadataPrimary(link: IggyConnection, generation: number, deadline: number,
    signal: AbortSignal): Promise<boolean> {
    const replica = [...this.replicas.values()].find((slot) => slot.connection === link)?.replica;
    const believed = this.metadataReplica === undefined ? undefined : this.replicas.get(this.metadataReplica);
    if (replica !== this.metadataReplica && believed?.attached && believed.connection?.connected)
      return false;
    try {
      const attempt = await this.exchange(link, GET_CLUSTER_METADATA.code, () => this.vsrSession.encode(
        GET_CLUSTER_METADATA.code, GET_CLUSTER_METADATA.serialize(),
        { ...EMPTY_PARTITION_CONTEXT, metadataOp: this.metadataWatermark }), deadline, signal);
      if (attempt.error) throw attempt.error;
      this.ensureGeneration(generation, GET_CLUSTER_METADATA.code);
      this.installRoster(attempt.response!);
    } catch (error) {
      debug('roster read after a namespace miss failed', error);
      return false;
    }
    return replica !== undefined && replica === this.metadataReplica;
  }

  private rememberRoute(target: PartitionTarget, response: CommandResponse): CachedRoute | undefined {
    const route = deserializePartitionRoute(response.data);
    const group = groupKey(route.context, target);
    const cached = this.partitionRoutes.get(target.key);
    if (cached && cached.group !== group && route.context.metadataOp < cached.route.context.metadataOp)
      return undefined;
    let minimumView = cached?.group === group ? cached.minimumView : 0;
    for (const alias of this.partitionRoutes.values()) {
      if (alias.group !== group) continue;
      minimumView = Math.max(minimumView, alias.minimumView, alias.route.view);
      if (alias.route.view === route.view && alias.route.primaryReplica !== route.primaryReplica)
        minimumView = Math.max(minimumView, route.view + 1);
    }
    if (route.context.metadataOp < this.metadataWatermark || route.view < minimumView)
      return undefined;
    const entry = { route, group, minimumView: route.view, stale: false };
    for (const alias of this.partitionRoutes.values()) {
      if (alias.group !== group) continue;
      alias.minimumView = Math.max(alias.minimumView, route.view);
      if (alias.route.view < route.view) alias.stale = true;
    }
    this.partitionRoutes.delete(target.key);
    if (this.partitionRoutes.size >= MAX_PARTITION_ROUTES)
      this.partitionRoutes.delete(this.partitionRoutes.keys().next().value!);
    this.partitionRoutes.set(target.key, entry);
    this.emit('partitionRoute', { topic: target.query.subarray(0, -PARTITION_ID_SIZE).toString('hex'), count: route.partitionsCount });
    return entry;
  }

  private invalidateRoute(refused: CachedRoute, advanceView: boolean): void {
    const floor = refused.route.view + (advanceView ? 1 : 0);
    for (const route of this.partitionRoutes.values()) {
      if (route.group !== refused.group || route.route.view > refused.route.view) continue;
      route.stale = true;
      route.minimumView = Math.max(route.minimumView, floor);
    }
  }

  private async primaryLink(route: CachedRoute, deadline: number, signal: AbortSignal): Promise<IggyConnection> {
    for (;;) {
      const slot = this.replicas.get(route.route.primaryReplica);
      if (slot?.attached && slot.connection?.connected) return slot.connection;
      if (slot?.failure instanceof ResponseError) throw slot.failure;
      if (slot?.failure instanceof TypeError) throw slot.failure;
      if (!slot || !slot.initial) {
        route.stale = true;
        const error = responseError(COMMAND_CODE.GetPartitionRoute, TRANSIENT_NOT_ACCEPTED);
        error.fromServer = true;
        throw error;
      }
      try { await waitWithin(this.changed.promise, deadline, signal); }
      catch (error) {
        if (!(error instanceof VsrResponseTimeoutError)) throw error;
        const refusal = responseError(COMMAND_CODE.GetPartitionRoute, TRANSIENT_NOT_ACCEPTED);
        refusal.fromServer = true;
        throw refusal;
      }
    }
  }

  private async readyLink(deadline: number, signal: AbortSignal, avoid?: IggyConnection): Promise<IggyConnection> {
    for (;;) {
      if (avoid) {
        const ready = [...this.replicas.values()].filter((slot) => slot.attached && slot.connection?.connected);
        const previous = ready.findIndex((slot) => slot.connection === avoid);
        if (previous >= 0) return ready[(previous + 1) % ready.length].connection!;
      }
      const primary = this.metadataReplica === undefined ? undefined : this.replicas.get(this.metadataReplica);
      if (primary?.attached && primary.connection?.connected && primary.connection !== avoid) return primary.connection;
      for (const slot of this.replicas.values())
        if (slot.attached && slot.connection?.connected && slot.connection !== avoid) return slot.connection;
      if (this.bootstrap?.connected && !this.bootstrapPromise) return this.bootstrap;
      if (!this.isAuthenticated || this.replicas.size === 0) return await this.connectBootstrap(deadline, signal);
      if ([...this.replicas.values()].every((slot) => slot.failure))
        throw [...this.replicas.values()][0]?.failure ?? new Error('no replica endpoints');
      await waitWithin(this.changed.promise, deadline, signal);
    }
  }

  private async refreshRoster(deadline: number, signal: AbortSignal): Promise<void> {
    if (!this.rosterPromise) {
      const generation = this.routingGeneration;
      const ownerSignal = AbortSignal.any([this.lifetime.signal, this.sessionLifetime.signal]);
      const pending = (async () => {
        const link = await this.readyLink(Date.now() + OPERATION_TIMEOUT_MS, ownerSignal);
        const response = await this.control(link, GET_CLUSTER_METADATA.code, GET_CLUSTER_METADATA.serialize(),
          Date.now() + OPERATION_TIMEOUT_MS, ownerSignal);
        this.ensureGeneration(generation, GET_CLUSTER_METADATA.code);
        this.installRoster(response);
      })().finally(() => { if (this.rosterPromise === pending) this.rosterPromise = undefined; });
      this.rosterPromise = pending;
    }
    await waitWithin(this.rosterPromise, deadline, signal);
  }

  private installRoster(response: CommandResponse): void {
    const metadata = GET_CLUSTER_METADATA.deserialize(response);
    if (metadata.nodes.length === 0 || metadata.nodes.length > MAX_REPLICAS || new Set(metadata.nodes.map((node) => node.replicaId)).size !== metadata.nodes.length)
      throw responseError(GET_CLUSTER_METADATA.code, INVALID_COMMAND, 'invalid replica roster');
    metadata.nodes = metadata.nodes.filter((node) => node.endpoints.tcp !== 0);
    this.metadataReplica = metadata.nodes.find((node) => node.role === 'Leader' && node.status === 'Healthy')?.replicaId;
    this.knownEndpoints = metadata.nodes.map((node) => ({ host: node.ip, port: node.endpoints.tcp }));
    const present = new Set(metadata.nodes.map((node) => node.replicaId));
    for (const [replica, slot] of this.replicas) {
      const endpoint = metadata.nodes.find((node) => node.replicaId === replica);
      if (!present.has(replica) || !endpoint || endpointKey(slot.endpoint) !== endpointKey({ host: endpoint.ip, port: endpoint.endpoints.tcp })) {
        slot.lifetime.abort();
        slot.connection?._destroy();
        this.replicas.delete(replica);
      }
    }
    // A single node can advertise an address the client cannot reach, such as
    // one behind a port mapping or a port-forward, so its slot dials the
    // configured endpoint.
    const single = metadata.nodes.length === 1;
    for (const node of metadata.nodes) {
      if (this.replicas.has(node.replicaId)) continue;
      const endpoint = { host: node.ip, port: node.endpoints.tcp };
      const slot: ReplicaSlot = {
        replica: node.replicaId, endpoint, dialEndpoint: single ? this.configuredEndpoint : endpoint,
        generation: this.routingGeneration, lifetime: new AbortController(), attached: false, initial: true,
        heartbeat: false, restartable: false
      };
      if (this.bootstrap?.connected && (single || this.bootstrap.isConnectedTo(endpoint.host, endpoint.port))) {
        slot.connection = this.bootstrap;
        slot.attached = true;
        slot.initial = false;
        this.bootstrap = undefined;
      }
      this.replicas.set(slot.replica, slot);
      void this.runReplica(slot);
    }
    if (this.bootstrap && metadata.nodes.length > 0) {
      this.bootstrap._destroy();
      this.bootstrap = undefined;
    }
    this.changed.pulse();
  }

  private async runReplica(slot: ReplicaSlot): Promise<void> {
    const signal = AbortSignal.any([this.lifetime.signal, this.sessionLifetime.signal, slot.lifetime.signal]);
    const reconnect = this.options.reconnect ?? { enabled: true, interval: 5_000, maxRetries: 12 };
    let failures = 0;
    try {
      for (;;) {
        signal.throwIfAborted();
        if (!slot.connection) {
          let permit = false;
          try {
            while (this.activeDials >= MAX_CONCURRENT_DIALS)
              await waitWithin(this.changed.promise, Date.now() + OPERATION_TIMEOUT_MS, signal);
            signal.throwIfAborted();
            this.activeDials += 1;
            permit = true;
            const connection = this.connectionFor(slot.dialEndpoint);
            slot.connection = connection;
            await connection.connect(signal);
            this.activeDials -= 1;
            permit = false;
            this.changed.pulse();
            const body = this.vsrSession.resumePayload(this.metadataWatermark);
            if (!body) throw responseError(COMMAND_CODE.BindSession, UNAUTHENTICATED);
            const response = await this.control(connection, COMMAND_CODE.BindSession, body,
              Date.now() + OPERATION_TIMEOUT_MS, signal);
            this.ensureGeneration(slot.generation, COMMAND_CODE.BindSession);
            if (readRegisteredSession(response) !== this.vsrSession.sessionId)
              throw responseError(COMMAND_CODE.BindSession, INVALID_SESSION_BINDING);
            slot.attached = true;
            failures = 0;
            this.changed.pulse();
          } catch (error) {
            slot.connection?._destroy();
            if (isSessionFailure(error, true)) {
              this.recoverSession(slot.generation);
              return;
            }
            // A 30 refusal failed only the link, so the slot binds again.
            if (error instanceof TypeError || (error instanceof ResponseError &&
                !isTransient(error.errorCode) && error.errorCode !== STALE_CLIENT)) {
              slot.failure = error;
              return;
            }
          } finally {
            if (permit) this.activeDials -= 1;
            slot.initial = false;
            this.changed.pulse();
          }
        }
        if (slot.attached && slot.connection) {
          await waitWithin(slot.connection.closed, Number.MAX_SAFE_INTEGER, signal);
          if (isSessionFailure(slot.connection.failure)) {
            this.recoverSession(slot.generation);
            return;
          }
        }
        slot.attached = false;
        slot.connection?._destroy();
        slot.connection = undefined;
        this.changed.pulse();
        if (!reconnect.enabled || failures >= reconnect.maxRetries) {
          slot.restartable = reconnect.enabled;
          slot.failure = this.budgetFailure(slot);
          return;
        }
        const base = Math.min(reconnect.interval * 2 ** Math.min(failures, 16), MAX_RECONNECT_INTERVAL_MS);
        failures += 1;
        await pause(Math.floor(Math.random() * (base + 1)), signal, true);
      }
    } catch (error) {
      if (!signal.aborted) slot.failure = error;
    } finally {
      slot.attached = false;
      slot.connection?._destroy();
      this.changed.pulse();
    }
  }

  /**
   * Only a heartbeat restarts a link that used its budget, so without one the
   * link stays down for good.
   */
  private budgetFailure(slot: ReplicaSlot): Error {
    return slot.restartable && this.heartbeatIntervalHandler ? new NotConnectedError() : new ReconnectExhaustedError();
  }

  private connectionFor(endpoint: Endpoint): IggyConnection {
    const connection = new IggyConnection({ ...this.options, options: { ...this.options.options, ...endpoint } });
    connection.on('eviction', (error) => this.emit('eviction', error));
    return connection;
  }

  private async connectBootstrap(deadline: number, signal: AbortSignal): Promise<IggyConnection> {
    if (this.bootstrap?.connected && !this.bootstrapPromise) return this.bootstrap;
    if (!this.bootstrapPromise) {
      const generation = this.routingGeneration;
      const ownerSignal = AbortSignal.any([this.lifetime.signal, this.sessionLifetime.signal]);
      this.bootstrapFailure = undefined;
      const pending = (async () => {
        const endpoints = [...new Map([this.configuredEndpoint, ...this.knownEndpoints].map((endpoint) => [endpointKey(endpoint), endpoint])).values()];
        const reconnect = this.options.reconnect ?? { enabled: true, interval: 5_000, maxRetries: 12 };
        let dialed = false;
        for (let pass = 0; ; pass += 1) {
          for (const endpoint of endpoints) {
            const link = this.connectionFor(endpoint);
            this.bootstrap = link;
            let accepted = false;
            try {
              await link.connect(ownerSignal);
              accepted = dialed = true;
              this.bootstrapFailure = undefined;
              if (this.isAuthenticated) {
                const body = this.vsrSession.resumePayload(this.metadataWatermark);
                if (!body) throw responseError(COMMAND_CODE.BindSession, UNAUTHENTICATED);
                const response = await this.control(link, COMMAND_CODE.BindSession, body,
                  Date.now() + OPERATION_TIMEOUT_MS, ownerSignal);
                if (readRegisteredSession(response) !== this.vsrSession.sessionId)
                  throw responseError(COMMAND_CODE.BindSession, INVALID_SESSION_BINDING);
              }
              return link;
            } catch (error) {
              link._destroy();
              if (this.isAuthenticated && isSessionFailure(error, true)) {
                this.recoverSession(generation);
                throw error;
              }
              if (error instanceof TypeError || ownerSignal.aborted) throw error;
              if (accepted) this.bootstrapFailure = error;
              else if (!dialed) this.bootstrapFailure = new NotConnectedError(error);
            }
          }
          if (!reconnect.enabled || pass >= reconnect.maxRetries)
            throw this.bootstrapFailure;
          const base = Math.min(reconnect.interval * 2 ** Math.min(pass, 16), MAX_RECONNECT_INTERVAL_MS);
          await pause(Math.floor(Math.random() * (base + 1)), ownerSignal, true);
        }
      })().finally(() => { if (this.bootstrapPromise === pending) this.bootstrapPromise = undefined; });
      this.bootstrapPromise = pending;
    }
    try {
      return await waitWithin(this.bootstrapPromise, deadline, signal);
    } catch (error) {
      // With the default reconnect values the deadline usually ends the dials
      // before the retry budget does, so it reports the last failure the same way.
      if (error instanceof VsrResponseTimeoutError && this.bootstrapFailure !== undefined)
        throw this.bootstrapFailure;
      throw error;
    }
  }

  private serializeSession<T>(action: () => Promise<T>): Promise<T> {
    const pending = this.sessionAction.then(action, action);
    this.sessionAction = pending.catch(() => undefined);
    return pending;
  }

  private async login(command: number, body: Buffer, deadline: number, signal: AbortSignal, lineage: number): Promise<CommandResponse> {
    this.ensureLineage(lineage, command);
    signal.throwIfAborted();
    if (this.isAuthenticated)
      await this.drive(LOGOUT.code, LOGOUT.serialize(), {}, deadline,
        AbortSignal.any([signal, this.sessionLifetime.signal]), lineage);
    this.ensureLineage(lineage, command);
    this.resetSession();
    const generation = this.routingGeneration;
    const sessionSignal = AbortSignal.any([signal, this.sessionLifetime.signal]);
    const prepared = prepareVsrCommand(command, body, this.vsrSession.bindSecret);
    let frame: Buffer | undefined;
    let retryDelay = RETRY_INTERVAL_MS;
    for (;;) {
      const link = await this.connectBootstrap(deadline, sessionSignal);
      this.ensureLineage(lineage, command);
      const attempt = await this.exchange(link, prepared.command,
        () => frame ??= this.vsrSession.encode(prepared.command, prepared.payload), deadline, sessionSignal);
      this.ensureLineage(lineage, command);
      this.ensureGeneration(generation, command);
      if (!attempt.error) {
        this.vsrSession.bind(readRegisteredSession(attempt.response!));
        this.userId = attempt.response!.data.readUInt32LE(0);
        this.isAuthenticated = true;
        this.heartbeat(this.options.heartbeatInterval);
        await this.refreshRoster(deadline, sessionSignal);
        this.ensureLineage(lineage, command);
        this._rememberCredentials(command, body);
        return attempt.response!;
      }
      if (attempt.error instanceof ResponseError && !isTransient(attempt.error.errorCode)) {
        throw attempt.error;
      }
      if (attempt.error instanceof VsrEvictionError || attempt.error instanceof TypeError) throw attempt.error;
      await waitWithin(pause(Math.floor(Math.random() * (retryDelay + 1)), sessionSignal), deadline, sessionSignal);
      retryDelay = Math.min(retryDelay * 2, MAX_RETRY_INTERVAL_MS);
    }
  }

  private recoverSession(generation: number): void {
    if (generation !== this.routingGeneration || this.recoveryPromise || this.lifetime.signal.aborted) return;
    this.resetSession();
    this.authenticationPromise = undefined;
    this.recoveryPromise = this.signIn().then(() => undefined)
      .catch((error) => { debug('session recovery failed', error); })
      .finally(() => { this.recoveryPromise = undefined; });
  }

  /**
   * Waits for the sign-in that runs or is queued now, and fails unless it binds
   * a session of the call's lineage. Only the client's own sign-in continues a
   * call: after a sign-in or sign-out of the caller, or a refused sign-in, the
   * call ends with 40, and at its deadline with 58.
   */
  private async nextSession(command: number, lineage: number, deadline: number, signal: AbortSignal): Promise<void> {
    let failure: unknown;
    if (lineage === this.lineage) {
      try {
        await waitWithin(this.authenticationPromise ?? this.sessionAction, deadline, signal);
      } catch (error) {
        signal.throwIfAborted();
        failure = error;
      }
    }
    if (lineage !== this.lineage) throw unauthenticated(command, failure);
    if (failure instanceof VsrResponseTimeoutError) throw responseError(command, TRANSIENT_NOT_ACCEPTED);
    if (!this.isAuthenticated && !this.signInCredentials) throw unauthenticated(command, failure);
    if (failure !== undefined) throw failure;
    if (!this.isAuthenticated)
      throw responseError(command, TRANSIENT_NOT_ACCEPTED);
  }

  /**
   * The `sessionReset` event says whether the reset ended a signed-in session.
   * A reset after a refused sign-in ends none.
   */
  private resetSession(): void {
    const signedIn = this.isAuthenticated;
    const hadActivity = signedIn || this.vsrSession.hasActivity;
    this.routingGeneration += 1;
    this.sessionLifetime.abort(responseError(0, TRANSIENT_NOT_ACCEPTED));
    this.sessionLifetime = new AbortController();
    this.lanes.reset();
    for (const slot of this.replicas.values()) {
      slot.lifetime.abort();
      slot.connection?._destroy();
    }
    this.replicas.clear();
    this.bootstrap?._destroy();
    this.bootstrap = undefined;
    this.bootstrapPromise = undefined;
    this.rosterPromise = undefined;
    this.partitionRoutes.clear();
    this.metadataReplica = undefined;
    this.isAuthenticated = false;
    this.userId = undefined;
    this.vsrSession.reset();
    clearInterval(this.heartbeatIntervalHandler);
    this.heartbeatIntervalHandler = undefined;
    this.changed.pulse();
    if (hadActivity) this.emit('sessionReset', signedIn);
  }

  private ensureLineage(lineage: number, command: number): void {
    if (lineage !== this.lineage) throw responseError(command, UNAUTHENTICATED);
  }

  private ensureGeneration(generation: number, command: number): void {
    if (generation !== this.routingGeneration || this.lifetime.signal.aborted)
      throw responseError(command, TRANSIENT_NOT_ACCEPTED);
  }

  private observeMetadata(frame: Buffer): void {
    const operation = readReplyOperation(frame);
    if (!hasCommittedResult(frame) || !(isMetadata(operation) || operation === Operation.DeleteSegments || operation === Operation.Register)) return;
    const commit = frame.readBigUInt64LE(REPLY_OFFSET.commit);
    if (commit > this.metadataWatermark) this.metadataWatermark = commit;
  }

  private _rememberCredentials(command: number, payload: Buffer): void {
    if (command === LOGIN.code) {
      const username = readWireName(payload, 0);
      this.signInCredentials = {
        username: username.value, password: readWireName(payload, username.next).value
      };
    } else if (command === LOGIN_WITH_TOKEN.code) {
      this.signInCredentials = { token: readWireName(payload, 0).value };
    } else if ((command === COMMAND_CODE.ChangePassword ||
        (command === COMMAND_CODE.UpdateResource && payload[0] === OptionsScope.User)) &&
        this.userId !== undefined && this.signInCredentials &&
        'username' in this.signInCredentials) {
      const credentials = this.signInCredentials;
      // An update leads with its scope byte, a password change with the user id.
      const start = command === COMMAND_CODE.ChangePassword ? 0 : 1;
      const identifier = [serializeIdentifier(this.userId), serializeIdentifier(credentials.username)]
        .find((encoded) => payload.subarray(start, start + encoded.length).equals(encoded));
      if (!identifier)
        return;
      if (command === COMMAND_CODE.ChangePassword) {
        const currentPassword = readWireName(payload, identifier.length);
        const password = readWireName(payload, currentPassword.next).value;
        this.signInCredentials = { ...credentials, password };
        return;
      }
      let position = start + identifier.length;
      const resetCount = payload.readUInt8(position);
      position += 1;
      for (let index = 0; index < resetCount; index++)
        position = readWireName(payload, position).next;
      const username = deserializeOptions(payload, position).username;
      if (typeof username !== 'string')
        return;
      this.signInCredentials = { ...credentials, username };
    }
  }

  hold(): () => void {
    this.pendingSubmissions += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.pendingSubmissions -= 1;
      if (this.pendingSubmissions === 0) this.emit('finishQueue');
    };
  }

  async authenticate(credentials: ClientCredentials): Promise<boolean> {
    if (credentials && typeof credentials === 'object' && 'token' in credentials)
      await this._authWithToken(credentials);
    else await this._authWithPassword(credentials as PasswordCredentials);
    return true;
  }

  private async loginByCaller(command: number, payload: () => Buffer, options: SendCommandOptions = {}): Promise<CommandResponse> {
    const lineage = ++this.sessionLineage;
    this.signInCredentials = undefined;
    const release = this.hold();
    const deadline = options.deadline ?? Date.now() + OPERATION_TIMEOUT_MS;
    const signal = AbortSignal.any([this.lifetime.signal, ...(options.signal ? [options.signal] : [])]);
    try {
      const body = payload();
      let admit!: () => void;
      const admitted = new Promise<void>((resolve) => { admit = resolve; });
      const pending = this.serializeSession(() => {
        admit();
        return this.login(command, body, deadline, signal, lineage);
      });
      const authentication = pending.then(() => true)
        .finally(() => { if (this.authenticationPromise === authentication) this.authenticationPromise = undefined; });
      this.authenticationPromise = authentication;
      void authentication.catch(() => undefined);
      // Bound the queue wait separately so dial timeouts keep their last transport error.
      await waitWithin(admitted, deadline, signal);
      return await pending;
    } catch (error) {
      if (lineage === this.lineage) {
        this.sessionLineage += 1;
        this.signInCredentials = undefined;
        this.resetSession();
      }
      if (error instanceof VsrResponseTimeoutError)
        throw responseError(command, TRANSIENT_NOT_ACCEPTED);
      throw error;
    } finally { release(); }
  }

  /** Only SDK recovery shares a sign-in; callers always own a fresh attempt. */
  private signIn(): Promise<boolean> {
    if (this.isAuthenticated) return Promise.resolve(true);
    if (!this.authenticationPromise) {
      const lineage = this.lineage;
      const identity = this.signInCredentials;
      const pending = this.serializeSession(async () => {
        this.ensureLineage(lineage, LOGIN.code);
        if (this.isAuthenticated) return true;
        if (!identity) return false;
        const command = 'token' in identity ? LOGIN_WITH_TOKEN : LOGIN;
        try {
          const body = 'token' in identity ? LOGIN_WITH_TOKEN.serialize(identity) : LOGIN.serialize(identity);
          await this.login(command.code, body, Date.now() + OPERATION_TIMEOUT_MS, this.lifetime.signal, lineage);
          return this.isAuthenticated;
        } catch (error) {
          if (lineage === this.lineage && (error instanceof TypeError || error instanceof VsrEvictionError ||
              (error instanceof ResponseError && !isTransient(error.errorCode))))
            this.signInCredentials = undefined;
          throw error;
        }
      }).finally(() => { if (this.authenticationPromise === pending) this.authenticationPromise = undefined; });
      this.authenticationPromise = pending;
    }
    return this.authenticationPromise;
  }

  async _authWithPassword(credentials: PasswordCredentials) {
    return LOGIN.deserialize(await this.loginByCaller(LOGIN.code, () => LOGIN.serialize(credentials)));
  }

  async _authWithToken(credentials: TokenCredentials) {
    return LOGIN_WITH_TOKEN.deserialize(await this.loginByCaller(LOGIN_WITH_TOKEN.code, () => LOGIN_WITH_TOKEN.serialize(credentials)));
  }

  async ping() {
    return PING.deserialize(await this.sendCommand(PING.code, PING.serialize()));
  }

  heartbeat(interval?: number): void {
    clearInterval(this.heartbeatIntervalHandler);
    this.heartbeatIntervalHandler = undefined;
    if (interval && this.isAuthenticated) {
      this.heartbeatIntervalHandler = setInterval(() => {
        for (const slot of this.replicas.values()) {
          // A retry budget limits one reconnect burst, not the life of the
          // session, so a long outage cannot leave the client down for good.
          if (slot.restartable) {
            slot.restartable = false;
            slot.failure = undefined;
            void this.runReplica(slot);
          }
          if (!slot.attached || !slot.connection?.connected || slot.heartbeat) continue;
          slot.heartbeat = true;
          const signal = AbortSignal.any([this.lifetime.signal, this.sessionLifetime.signal, slot.lifetime.signal]);
          void this.control(slot.connection, PING.code, PING.serialize(), Date.now() + OPERATION_TIMEOUT_MS, signal)
            .then(() => { this.emit('heartbeat'); })
            .catch((error) => {
              if (isSessionFailure(error)) this.recoverSession(slot.generation);
              debug('replica heartbeat failed', error);
            }).finally(() => { slot.heartbeat = false; });
        }
      }, interval);
      this.heartbeatIntervalHandler.unref();
    }
    // A link that used its budget reports whether a heartbeat restarts it.
    for (const slot of this.replicas.values())
      if (slot.restartable) slot.failure = this.budgetFailure(slot);
  }

  getReadStream() {
    const connection = this.bootstrap ?? [...this.replicas.values()].find((slot) => slot.attached)?.connection;
    if (!connection?.socket) throw new Error('client has no connected replica');
    return connection.socket;
  }

  destroy(): void {
    this.signInCredentials = undefined;
    this.lifetime.abort(new Error('client destroyed'));
    this.resetSession();
  }
}

export function getRawClient(options: ClientConfigOrString): RawClient {
  return new CommandResponseStream(options);
}

const isLoginCommand = (command: number): boolean =>
  command === COMMAND_CODE.LoginUser || command === COMMAND_CODE.LoginWithAccessToken;
const isTransient = (code: number): boolean =>
  code === TRANSIENT_NOT_COMMITTED || code === TRANSIENT_NOT_ACCEPTED || code === NOT_PRIMARY;
/** A 40 that keeps why the call has no session, such as a refused sign-in. */
const unauthenticated = (command: number, cause: unknown): ResponseError => {
  const error = responseError(command, UNAUTHENTICATED);
  if (cause !== undefined) error.cause = cause;
  return error;
};
/** An unknown outcome. Only a retainable operation has an ID to resolve, any other call ends with 57. */
const uncertainOutcome = (command: number, operation: ClientOperation | undefined, cause: unknown): Error => {
  if (operation?.retainable) return new OperationUncertainError(operation.id, cause);
  const error = responseError(command, TRANSIENT_NOT_COMMITTED);
  error.cause = cause;
  return error;
};
const isDetachedFailure = (error: unknown): boolean =>
  error instanceof VsrEvictionError
    ? error.reason === EvictionReason.NoSession || error.reason === EvictionReason.StaleClient
    : error instanceof ResponseError && error.fromServer && !error.committed &&
      (error.errorCode === UNAUTHENTICATED || error.errorCode === STALE_CLIENT);
const isSessionFailure = (error: unknown, binding = false): boolean =>
  (error instanceof VsrEvictionError && (binding || !isDetachedFailure(error))) ||
  (error instanceof ResponseError && error.fromServer &&
    ((error.errorCode === UNAUTHENTICATED && (binding || error.committed)) ||
      (error.errorCode === STALE_CLIENT && error.committed)));
const groupKey = (context: PartitionContext, target: PartitionTarget): string =>
  `${context.incarnation}:${target.query.readUInt32LE(target.query.length - PARTITION_ID_SIZE)}`;

type PartitionTarget = {
  query: Buffer,
  key: string
};

const partitionRoute = (command: number, payload: Buffer): PartitionTarget | undefined => {
  if (command === COMMAND_CODE.SendMessages) {
    const { query } = sendTarget(payload);
    return { query, key: query.toString('hex') };
  }
  if (command !== COMMAND_CODE.PollMessages && command !== COMMAND_CODE.GetOffset &&
      command !== COMMAND_CODE.StoreOffset && command !== COMMAND_CODE.DeleteConsumerOffset)
    return undefined;
  let position = 1;
  const consumerEnd = identifierEnd(payload, position, command);
  const streamEnd = identifierEnd(payload, consumerEnd, command);
  position = identifierEnd(payload, streamEnd, command);
  const suffix = command === COMMAND_CODE.PollMessages ? POLL_OPTIONS_SIZE
    : command === COMMAND_CODE.StoreOffset ? STORE_OFFSET_SUFFIX_SIZE
      : command === COMMAND_CODE.DeleteConsumerOffset ? DELETE_OFFSET_SUFFIX_SIZE : 0;
  if (payload.length !== position + 1 + PARTITION_ID_SIZE + suffix || payload[position] > 1)
    throw responseError(command, INVALID_COMMAND);
  if (payload[position] === 0) {
    if (command === COMMAND_CODE.StoreOffset || command === COMMAND_CODE.DeleteConsumerOffset ||
        (command === COMMAND_CODE.PollMessages && payload[0] === ConsumerKind.Group))
      throw responseError(command, INVALID_COMMAND);
  }
  const query = Buffer.concat([payload.subarray(consumerEnd, position),
    payload[position] === 0 ? Buffer.alloc(PARTITION_ID_SIZE)
      : payload.subarray(position + 1, position + 1 + PARTITION_ID_SIZE)]);
  return { query, key: query.toString('hex') };
};

const identifierEnd = (payload: Buffer, position: number, command: number): number => {
  if (position + IDENTIFIER_PREFIX_SIZE > payload.length)
    throw responseError(command, INVALID_COMMAND);
  const end = position + IDENTIFIER_PREFIX_SIZE + payload[position + 1];
  if (end > payload.length)
    throw responseError(command, INVALID_COMMAND);
  return end;
};

type SendTarget = {
  /** `GetPartitionRoute` body: stream, topic and partition id */
  query: Buffer
};

/**
 * Reads the explicit partition a raw send writes to. A Balanced or MessageKey
 * send names no partition, so there is no context to capture for it, and the
 * server refuses a send without one.
 */
const sendTarget = (payload: Buffer): SendTarget => {
  let position = SEND_METADATA_LENGTH_SIZE;
  for (let identifier = 0; identifier < 2; identifier++) {
    if (position + IDENTIFIER_PREFIX_SIZE > payload.length)
      throw responseError(COMMAND_CODE.SendMessages, INVALID_COMMAND);
    position += IDENTIFIER_PREFIX_SIZE + payload[position + 1];
  }
  if (position + IDENTIFIER_PREFIX_SIZE > payload.length)
    throw responseError(COMMAND_CODE.SendMessages, INVALID_COMMAND);
  if (payload[position] !== PartitionKind.PartitionId)
    throw responseError(COMMAND_CODE.SendMessages, FEATURE_UNAVAILABLE, RAW_SEND_NEEDS_PARTITION);
  const partition = position + IDENTIFIER_PREFIX_SIZE;
  if (payload[position + 1] !== PARTITION_ID_SIZE || partition + PARTITION_ID_SIZE > payload.length)
    throw responseError(COMMAND_CODE.SendMessages, INVALID_COMMAND);
  const identifiers = payload.subarray(SEND_METADATA_LENGTH_SIZE, position);
  return {
    query: Buffer.concat([identifiers, payload.subarray(partition, partition + PARTITION_ID_SIZE)])
  };
};
