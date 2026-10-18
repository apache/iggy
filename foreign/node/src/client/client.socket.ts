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
  ClientConfig,
  ClientConfigOrString,
  ClientCredentials, CommandResponse,
  PasswordCredentials, RawClient, SendCommandOptions,
  TokenCredentials
} from '../client/client.type.js';
import { ResponseError, responseError } from '../wire/error.utils.js';
import { debug } from './client.debug.js';
import {
  endpointKey,
  type Endpoint,
  IggyConnection
} from './client.connection.js';
import { LOGIN, LOGIN_WITH_TOKEN, LOGOUT, PING } from '../wire/index.js';
import { GET_CLUSTER_METADATA } from '../wire/cluster/get-cluster-metadata.command.js';
import { deserializePartitionRoute, type PartitionRoute } from '../wire/partition/partition-route.js';
import { ConsumerKind } from '../wire/offset/offset.utils.js';
import { COMMAND_CODE } from '../wire/command.code.js';
import { serializeIdentifier } from '../wire/identifier.utils.js';
import { PartitionKind } from '../wire/message/partitioning.utils.js';
import {
  Command, HEADER_SIZE, REPLY_OFFSET, type PartitionContext, peekCommand, readReplyOperation, readStatus
} from '../wire/vsr/header.js';
import { Operation, isKnownOperation } from '../wire/vsr/operation.js';
import {
  decodeVsrResponse,
  prepareVsrCommand,
  readRegisteredSession,
  readWireName,
  VsrEvictionError,
  VsrSession,
} from '../wire/vsr/index.js';
import { normalizeClientConfig } from './client.config.js';

const VSR_RESPONSE_TIMEOUT_MS = 30_000;
const VSR_RETRY_INTERVAL_MS = 50;
const LEADERLESS_WAIT_BUDGET_MS = 5_000;
const LEADERLESS_POLL_INTERVAL_MS = 250;
const MAX_LEADER_REDIRECTS = 3;
const TRANSIENT_NOT_COMMITTED = 57;
const TRANSIENT_NOT_ACCEPTED = 58;
const UNAUTHENTICATED = 40;
const STALE_CLIENT = 30;
const HISTORY_UNAVAILABLE = 87;
const LIFECYCLE_BUSY = 88;
const NOT_PRIMARY = 89;
const INVALID_COMMAND = 3;
const FEATURE_UNAVAILABLE = 5;
/** Each lifecycle retry commits another refusal, so its pause doubles up to this. */
const LIFECYCLE_RETRY_MAX_INTERVAL_MS = 1_000;
const MAX_PARTITION_ROUTES = 4096;
const MAX_POLL_CONNECTIONS = 256;
const POLL_OPTIONS_SIZE = 9 + 4 + 1;
const IDENTIFIER_PREFIX_SIZE = 2;
const SEND_METADATA_LENGTH_SIZE = 4;
const PARTITION_ID_SIZE = 4;
const STORE_OFFSET_SUFFIX_SIZE = 8 + 1;
const DELETE_OFFSET_SUFFIX_SIZE = 1;
const RAW_SEND_NEEDS_PARTITION =
  'Raw sends need an explicit partition id. Use the typed send API for Balanced or MessageKey partitioning.';

/**
 * The client's own commands after which every cached send context is dropped,
 * as the Rust SDK drops its topic discovery. Names and numeric ids can key the
 * same topic twice, so a single entry cannot be singled out.
 */
const TOPIC_DISCOVERY_RESETS = new Set<number>([
  COMMAND_CODE.DeleteStream,
  COMMAND_CODE.DeleteTopic,
  COMMAND_CODE.CreatePartitions,
  COMMAND_CODE.DeletePartitions
]);

type PollConnection = {
  client: CommandResponseStream,
  metadataWatermark?: bigint,
  busy?: Promise<void>,
};

type RoutedAttempt = { frame?: Buffer, clientId?: bigint };

/**
 * How long a `TRANSIENT_NOT_ACCEPTED` request replays on the same connection
 * before the roster is re-read. A node that stopped being primary refuses
 * forever, so replaying alone never recovers. Matches the Rust SDK.
 */
const VSR_FAILOVER_CHECK_MS = 2_000;

/**
 * Whether a request's budget still holds enough for another attempt. One
 * exchange needs at least a replay interval to be worth starting; below that
 * the attempt can only end in a timeout, which would hide the refusal that
 * actually came back.
 */
const worthAnotherAttempt = (deadline: number): boolean =>
  deadline - Date.now() > VSR_RETRY_INTERVAL_MS;

/**
 * A request the current node keeps refusing as not-admitted. Carries the
 * refusal so the caller can surface it when the roster turns out to still name
 * this node as the leader. Never escapes `sendCommand`.
 */
class LeaderMovedError extends Error {
  constructor(readonly refusal: ResponseError) {
    super('the node refused the request as not-admitted; re-reading the roster');
  }
}

/**
 * How a leader move resolved: onto the metadata leader, onto the next roster
 * node (for a request the metadata leader itself keeps refusing), or not at
 * all.
 */
type RosterWalkVerdict = {
  endpoint: Endpoint,
  moved: boolean
};
type LeaderMoveVerdict = 'leader' | RosterWalkVerdict | false;

/**
 * Command codes that can be executed without authentication.
 */
const UNLOGGED_COMMAND_CODE = [
  PING.code,
  LOGIN.code,
  LOGIN_WITH_TOKEN.code
];

/**
 * Represents a queued command job waiting to be executed.
 */
type Job = {
  /** Partition context captured for the request, kept through its retries */
  context?: PartitionContext,
  attempt?: RoutedAttempt,
  /** Command code */
  command: number,
  /** Command payload */
  payload: Buffer,
  /** Whether to parse the response */
  handleResponse: boolean,
  /** Whether the command is appended rather than prepended to the queue */
  last: boolean,
  /** Whether a not-admitted refusal re-checks the leader and re-issues */
  followsLeaderMoves: boolean,
  /** When the whole request gives up, however often it is re-issued */
  deadline: number,
  /** Promise resolve function */
  resolve: (v: CommandResponse | PromiseLike<CommandResponse>) => void,
  /** Promise reject function */
  reject: (e: unknown) => void
};

type ExchangeState = {
  written: boolean
};

export class VsrResponseTimeoutError extends Error {
  constructor(timeout: number) {
    super(`timed out after ${timeout} ms waiting for VSR response`);
    this.name = 'VsrResponseTimeoutError';
    Object.setPrototypeOf(this, VsrResponseTimeoutError.prototype);
  }
}

/**
 * Manages command execution and response handling for the Iggy server.
 * Implements command queuing, authentication, and heartbeat functionality.
 */
export class CommandResponseStream extends EventEmitter {
  /** Client configuration */
  private options: ClientConfig;
  /** Underlying connection to the server */
  private connection: IggyConnection;
  /** Queue of pending command jobs */
  private _execQueue: Job[];
  /** Consensus session used by VSR framing */
  private vsrSession: VsrSession;
  /** Shared authentication attempt for concurrent callers */
  private authenticationPromise?: Promise<boolean>;
  /** Whether a login is already being moved to the leader */
  private settlingLeader: boolean;
  /**
   * The leader re-check a refused request started, shared with every other
   * request refused by the same node so one demotion moves the client once.
   */
  private leaderMoveInFlight?: Promise<LeaderMoveVerdict>;
  /**
   * Set when a roster walk just redirected this client, so the login that
   * re-authenticates it stays on the dialed node instead of settling back on
   * the metadata leader whose partition replica refused the request.
   */
  private walkSettleSuppressed = false;
  /**
   * Refusals handed out to callers that have not decided what to do with them
   * yet. The queue holds while any are outstanding: the caller of a refused
   * command re-checks the leader, and a command written in the meantime goes
   * out on the socket that check is about to replace.
   */
  private leaderMovesUndecided: number;
  /** How long a leaderless roster is polled before settling in place */
  private leaderlessWaitBudget: number;
  /** Delay between roster reads while the cluster elects */
  private leaderlessPollInterval: number;
  /** Calls that have acquired this stream but have not fully settled */
  private pendingSubmissions: number;
  /** Whether the stream is currently processing a command */
  public busy: boolean;
  /** Whether the client has been authenticated */
  isAuthenticated: boolean;
  /** Authenticated user ID */
  userId?: number;
  /** Heartbeat interval timer handle */
  heartbeatIntervalHandler?: NodeJS.Timeout;
  /** Whether a heartbeat ping is still awaiting its response */
  private heartbeatInFlight: boolean;
  private rememberedCredentials?: ClientCredentials;
  private clustered?: boolean;
  private metadataWatermark = 0n;
  private routingGeneration = 0;
  private partitionRoutes = new Map<string, PartitionRoute>();
  private replicaEndpoints = new Map<number, Endpoint>();
  private pollConnections = new Map<string, PollConnection>();
  /**
   * A connection to a partition primary. Each request on it gets one attempt:
   * its caller routes again instead, as the Rust SDK does.
   */
  private dataConnection = false;

  /**
   * Creates a new CommandResponseStream.
   *
   * @param options - Client configuration
   */
  constructor(options: ClientConfigOrString) {
    super();
    const normalizedConfig = normalizeClientConfig(options);
    this.options = normalizedConfig;
    this.connection = new IggyConnection(normalizedConfig);
    this.busy = false;
    this.isAuthenticated = false;
    this._execQueue = [];
    this.vsrSession = new VsrSession();
    this.authenticationPromise = undefined;
    this.settlingLeader = false;
    this.leaderMovesUndecided = 0;
    this.leaderlessWaitBudget = LEADERLESS_WAIT_BUDGET_MS;
    this.leaderlessPollInterval = LEADERLESS_POLL_INTERVAL_MS;
    this.pendingSubmissions = 0;
    this.heartbeatInFlight = false;
    this._init();
  };

  /**
   * Initializes the stream by setting up heartbeat and connection event handlers.
   */
  _init() {
    this.heartbeat(this.options.heartbeatInterval);
    this.connection.on('error', (error: Error) => {
      this._failQueue(error);
    });
    this.connection.on('eviction', (error: VsrEvictionError) => {
      this._resetSession();
      this.emit('eviction', error);
    });
    this.connection.on('disconnected', () => {
      this._resetSession();
      if (this.connection.redirecting) {
        // The client is moving to the leader, which is its own doing: a queued
        // command has not been written, so it belongs on the node being moved
        // to rather than in an error.
        this._reissueQueue();
        return;
      }
      this._failQueue(
        new Error('connection closed before queued commands were sent')
      );
    });
  }

  /**
   * Re-submits queued commands once the client has reconnected and signed in
   * again on the node it moved to.
   *
   * Only for a drop the client caused. Nothing here was written, so there is no
   * outcome in doubt: a command still in the queue when the socket is replaced
   * would otherwise fail with a lost-connection error the caller can do nothing
   * about.
   */
  private _reissueQueue(): void {
    const queued = this._execQueue;
    this._execQueue = [];
    for (const job of queued) {
      debug('re-issuing a queued command after a leader move', job.command);
      this._resubmit(job).then(job.resolve, job.reject);
    }
  }

  /**
   * The whole job goes back in the queue, not just its payload, and its caller
   * still owns every retry decision. A fresh budget would let a command caught
   * in a move take twice the response timeout, a roster read re-issued as
   * leader-following would answer a leader check with another leader check,
   * and a fresh context would commit work from one partition incarnation in
   * another or refresh a send twice.
   */
  private async _resubmit(job: Job): Promise<CommandResponse> {
    if (!this.connection.connected)
      await this.connection.connect();
    if (!this.isAuthenticated && !this.isUnloggedCommand(job.command))
      await this.authenticate(this._signInCredentials());
    return this._queueCommand(job.command, job.payload, job.handleResponse,
      job.last, job.followsLeaderMoves, job.deadline, job.context, job.attempt);
  }

  /**
   * Sends a command to the server.
   * Automatically handles connection, authentication and leader settlement.
   *
   * @param command - Command code to send
   * @param payload - Command payload buffer
   * @param options - Response and queue options
   * @returns Promise resolving to the command response
   */
  async sendCommand(
    command: number,
    payload: Buffer,
    options: SendCommandOptions = {}
  ): Promise<CommandResponse> {
    this.pendingSubmissions += 1;
    try {
      const {
        handleResponse = true,
        last = true,
        followsLeaderMoves = true
      } = options;
      payload = Buffer.from(payload);
      const route = partitionRoute(command, payload);
      const routeDeadline = route || command === COMMAND_CODE.GetPartitionRoute
        ? options.deadline ?? Date.now() + VSR_RESPONSE_TIMEOUT_MS
        : undefined;

      if (!this.connection.connected)
        await (routeDeadline === undefined ? this.connection.connect()
          : withinDeadline(this.connection.connect(), routeDeadline));

      if (!this.isAuthenticated && !this.isUnloggedCommand(command))
        await (routeDeadline === undefined ? this.authenticate(this._signInCredentials())
          : withinDeadline(this.authenticate(this._signInCredentials()), routeDeadline));

      if (route && routeDeadline !== undefined) {
        const deadline = routeDeadline;
        if (this.clustered === undefined)
          await withinDeadline(this.sendCommand(GET_CLUSTER_METADATA.code,
            GET_CLUSTER_METADATA.serialize(), { deadline, followsLeaderMoves: false }), deadline);
        if (this.clustered === undefined)
          throw new Error('cannot determine the partition routing topology');
        return await this._sendOnPrimary(command, route, payload, deadline,
          handleResponse, options.context);
      }

      // The roster read is itself a queued command and the queue is
      // single-flighted, so the leader re-check cannot happen inside
      // `_processVsr`. The refusal comes back out here instead, where the
      // queue is free, and the command is re-issued on the node that now
      // leads.
      //
      // A not-admitted refusal means the request was never applied, so it is
      // re-issued for the whole request budget rather than given up on after
      // one window: the roster can still name this node -- an election in
      // flight, a leader that has not moved yet -- and that is a wait, not a
      // verdict.
      //
      // One budget for the whole request: the transient replays on a
      // connection, the leader re-checks, and the re-issues after a move all
      // spend it, so a request cannot outlive it by moving. A command re-issued
      // after a move keeps the budget it was first submitted with, rather than
      // opening a second one.
      const deadline = options.deadline ?? Date.now() + VSR_RESPONSE_TIMEOUT_MS;
      const context = options.context;
      let lifecycleRetryInterval = VSR_RETRY_INTERVAL_MS;
      let response: CommandResponse;
      let walkingRoster = false;
      const visitedRosterEndpoints = new Set<string>();
      for (;;) {
        try {
          response = await this._queueCommand(command, payload, handleResponse,
            last, followsLeaderMoves, deadline, context);
          break;
        } catch (error) {
          // A lifecycle refusal is committed, so the request did not apply and
          // its retry is a new request. The pause runs here, outside the queue,
          // so it holds back no other command of this client.
          if (error instanceof ResponseError && error.errorCode === LIFECYCLE_BUSY &&
              Date.now() + lifecycleRetryInterval < deadline) {
            await delay(lifecycleRetryInterval);
            lifecycleRetryInterval = Math.min(lifecycleRetryInterval * 2, LIFECYCLE_RETRY_MAX_INTERVAL_MS);
            continue;
          }
          if (!(error instanceof LeaderMovedError))
            throw error;
          // The roster read that a re-check runs is itself a command that can
          // be refused this way, and answering a leader check with another
          // leader check would recurse. Its caller reads a failure as "stay
          // where you are".
          //
          // A budget too small to carry another attempt ends it here, with the
          // refusal the server actually gave: re-issued into what is left, the
          // request would time out instead and the caller would see a timeout
          // where the answer was "not admitted".
          let moved: LeaderMoveVerdict = false;
          try {
            if (!followsLeaderMoves || !worthAnotherAttempt(deadline))
              throw responseError(command, error.refusal.errorCode);
            // Once this request starts walking the roster it keeps walking: a
            // leader recheck between hops would put it straight back on the
            // metadata leader whose partition replica refused it, and the
            // walk would bounce between two nodes without reaching the rest.
            moved = await this._followLeaderMove(
              walkingRoster && error.refusal.errorCode !== NOT_PRIMARY,
              visitedRosterEndpoints,
              error.refusal.errorCode !== NOT_PRIMARY
            );
            if (typeof moved === 'object') {
              walkingRoster = true;
              visitedRosterEndpoints.add(endpointKey(moved.endpoint));
            }
          } finally {
            // Released as soon as the move is decided, before the pace below
            // and before any re-authentication: those go through the queue
            // themselves, and a queue still held for this refusal would never
            // reach them.
            if (followsLeaderMoves)
              this._releaseUndecidedMove();
          }
          const connectionMoved = moved === 'leader' ||
            (typeof moved === 'object' && moved.moved);
          if (!connectionMoved) {
            // Nowhere else to go yet: the roster still names this node, or it
            // could not be read. Paced, because the in-connection replay
            // window belongs to the request's budget and has already been
            // spent -- re-issuing straight away would spin.
            await delay(Math.min(
              VSR_FAILOVER_CHECK_MS,
              Math.max(0, deadline - Date.now())
            ));
          }
          if (!worthAnotherAttempt(deadline))
            throw responseError(command, error.refusal.errorCode);
          // A move drops the session with the socket it was bound to, so the
          // re-issue would otherwise go out under no session: a replicated
          // command fails client-side, a non-replicated one goes out with
          // session 0.
          if (!this.isAuthenticated && !this.isUnloggedCommand(command))
            await this.authenticate(this._signInCredentials());
        }
      }
      if (TOPIC_DISCOVERY_RESETS.has(command)) {
        this.partitionRoutes.clear();
        this.emit('topicDiscoveryReset');
      }
      if (!isLoginCommand(command) || this.settlingLeader)
        return response;
      // A login that re-authenticates a roster walk stays on the dialed node:
      // settling would put the client back on the metadata leader whose
      // partition replica refused the walked request. One login only.
      if (this.walkSettleSuppressed) {
        this.walkSettleSuppressed = false;
        return response;
      }
      this.settlingLeader = true;
      try {
        const settled = await this._settleOnLeader(command, payload);
        return settled ?? response;
      } finally {
        this.settlingLeader = false;
      }
    } finally {
      this.pendingSubmissions -= 1;
      this._emitFinishQueue();
    }
  }

  private async _lookupRoute(partition: PartitionTarget, deadline: number): Promise<PartitionRoute> {
    const cached = this.partitionRoutes.get(partition.key);
    if (cached && cached.context.metadataOp >= this.metadataWatermark)
      return cached;
    const response = await this._routingControl(partition, deadline);
    return deserializePartitionRoute(response.data);
  }

  private _rememberRoute(query: Buffer, response: CommandResponse): void {
    const route = deserializePartitionRoute(response.data);
    if (this.partitionRoutes.size >= MAX_PARTITION_ROUTES)
      this.partitionRoutes.clear();
    this.partitionRoutes.set(query.toString('hex'), route);
    this.emit('partitionRoute', { topic: query.subarray(0, -PARTITION_ID_SIZE).toString('hex'),
      count: route.partitionsCount });
  }

  private _queueCommand(
    command: number,
    payload: Buffer,
    handleResponse: boolean,
    last: boolean,
    followsLeaderMoves: boolean,
    deadline: number,
    context?: PartitionContext,
    attempt?: RoutedAttempt
  ): Promise<CommandResponse> {
    return new Promise<CommandResponse>((resolve, reject) => {
      const job: Job = {
        command,
        context,
        attempt,
        payload,
        handleResponse,
        last,
        followsLeaderMoves,
        deadline,
        resolve,
        reject
      };
      if (last)
        this._execQueue.push(job);
      else
        this._execQueue.unshift(job);
      this._processQueue();
    });
  }

  /**
   * Re-reads the roster and moves to the leader it names.
   *
   * Best effort: an unreadable roster, or one that still names this node,
   * leaves the client where it is and the refused request is re-issued anyway.
   *
   * Single-flighted, and concurrent callers share the outcome instead of
   * failing: several commands are refused by the same demoted node, and each
   * starting its own redirect would move the client once per command. The
   * first redirect's `'disconnected'` also fails the others' roster reads, so
   * a caller that raced one would report a refusal it never had to.
   *
   * @returns Whether the client moved
   */
  private _followLeaderMove(
    walkPastLeader = false,
    visitedRosterEndpoints = new Set<string>(),
    allowWalk = true
  ): Promise<LeaderMoveVerdict> {
    const inFlight = this.leaderMoveInFlight;
    if (inFlight)
      return inFlight;

    const move = (async (): Promise<LeaderMoveVerdict> => {
      try {
        if (!walkPastLeader) {
          const leader = await this._readLeaderEndpoint();
          if (leader && !this.connection.isConnectedTo(leader.host, leader.port)) {
            debug(`the leader moved to ${leader.host}:${leader.port}, following it`);
            await this.connection.redirect(leader.host, leader.port);
            return 'leader';
          }
        }
        // The roster names this node as the metadata leader (or said nothing
        // usable), yet it keeps refusing to admit the request: its replica of
        // the target partition group is not that group's primary, because
        // metadata and partition consensus groups elect independently. Walk
        // the roster instead of re-issuing into the same refusal.
        if (!allowWalk)
          return false;
        const next = this.connection.nextRosterEndpoint(visitedRosterEndpoints);
        if (!next)
          return false;
        debug(
          'the request keeps being refused here, walking the roster to ' +
          `${next.host}:${next.port}`
        );
        // The re-authentication after this redirect runs a login, and a login
        // normally settles on the metadata leader, which would put the walk
        // right back on the node that refused. One login only.
        this.walkSettleSuppressed = true;
        try {
          await this.connection.redirect(next.host, next.port);
        } catch (error) {
          // The suppression belongs to the redirect above. If that redirect
          // never lands, a later unrelated login must settle normally.
          this.walkSettleSuppressed = false;
          debug('the roster endpoint could not be reached', error);
          return { endpoint: next, moved: false };
        }
        return { endpoint: next, moved: true };
      } catch (error) {
        debug('the leader could not be re-checked, staying on this node', error);
        return false;
      }
    })();
    this.leaderMoveInFlight = move;
    void move.finally(() => {
      if (this.leaderMoveInFlight !== move)
        return;
      this.leaderMoveInFlight = undefined;
      // The drain stopped while the move was being decided. A move that
      // happened re-issues what was held back on the new socket; one that did
      // not leaves it here, with nothing else due to pick it up.
      if (!this.connection.redirecting)
        void this._processQueue();
    });
    return move;
  }

  /** Whether a leader move is being decided or carried out. */
  private _movePending(): boolean {
    return this.leaderMovesUndecided > 0 || this.leaderMoveInFlight !== undefined;
  }

  /**
   * Releases the queue hold one refusal took, and drains what was held back
   * once the last of them is decided.
   */
  private _releaseUndecidedMove(): void {
    if (this.leaderMovesUndecided > 0)
      this.leaderMovesUndecided -= 1;
    if (this._movePending() || this.connection.redirecting)
      return;
    void this._processQueue();
  }

  private _rememberRoster(response: CommandResponse): void {
    try {
      const metadata = GET_CLUSTER_METADATA.deserialize(response);
      if (metadata.nodes.length === 0)
        return;
      this.clustered = metadata.nodes.length > 1;
      this.replicaEndpoints = new Map(metadata.nodes
        .filter((node) => node.endpoints.tcp !== 0)
        .map((node) => [node.replicaId, { host: node.ip, port: node.endpoints.tcp }]));
      this.connection.rememberRoster(
        metadata.nodes
          .filter((node) => node.endpoints.tcp !== 0)
          .map((node) => ({ host: node.ip, port: node.endpoints.tcp }))
      );
    } catch (error) {
      debug('an unreadable roster leaves the redial candidates as they are',
        error);
    }
  }

  private async _sendOnPrimary(
    command: number,
    partition: PartitionTarget,
    payload: Buffer,
    deadline: number,
    handleResponse: boolean,
    context?: PartitionContext
  ): Promise<CommandResponse> {
    let captured = context;
    let refreshedHistory = false;
    let refusedPrimary = false;
    let lifecycleRetryInterval = VSR_RETRY_INTERVAL_MS;
    const attempt: RoutedAttempt = {};
    while (Date.now() < deadline && !this.connection.ending) {
      let missingReplica = false;
      try {
        const generation = this.routingGeneration;
        const route = await this._lookupRoute(partition, deadline);
        if (generation !== this.routingGeneration)
          throw responseError(command, TRANSIENT_NOT_ACCEPTED);
        captured ??= route.context;
        if (!this.clustered)
          return await this._queueCommand(command, payload, handleResponse, true, false,
            deadline, captured, attempt);
        let endpoint = this.replicaEndpoints.get(route.primaryReplica);
        if (!endpoint) {
          await this.sendCommand(GET_CLUSTER_METADATA.code, GET_CLUSTER_METADATA.serialize(),
            { deadline, followsLeaderMoves: false });
          endpoint = this.replicaEndpoints.get(route.primaryReplica);
          if (!endpoint) {
            missingReplica = true;
            throw responseError(command, TRANSIENT_NOT_ACCEPTED);
          }
        }
        const key = endpointKey(endpoint);
        let entry = this.pollConnections.get(key);
        if (!entry) {
          if (this.pollConnections.size >= MAX_POLL_CONNECTIONS) {
            const idle = Array.from(this.pollConnections).find(([, value]) => !value.busy);
            if (!idle)
              throw responseError(command, TRANSIENT_NOT_ACCEPTED);
            this.pollConnections.delete(idle[0]);
            idle[1].client.destroy();
          }
          entry = {
            client: new CommandResponseStream({
              ...this.options,
              options: { ...this.options.options, ...endpoint },
              heartbeatInterval: 0,
              reconnect: { enabled: false, interval: 0, maxRetries: 0 }
            })
          };
          entry.client.vsrSession = this.vsrSession;
          entry.client.dataConnection = true;
          this.pollConnections.set(key, entry);
        }
        while (entry.busy)
          await withinDeadline(entry.busy, deadline);
        if (generation !== this.routingGeneration || this.pollConnections.get(key) !== entry)
          throw responseError(command, TRANSIENT_NOT_ACCEPTED);
        let release!: () => void;
        entry.busy = new Promise<void>((resolve) => { release = resolve; });
        let sent = false;
        try {
          if (!entry.client.isAuthenticated) {
            entry.metadataWatermark = undefined;
            await withinDeadline(entry.client.connection.connect(true),
              Math.min(deadline, Date.now() + VSR_FAILOVER_CHECK_MS),
              () => entry.client.destroy());
          }
          const watermark = route.context.metadataOp > this.metadataWatermark
            ? route.context.metadataOp : this.metadataWatermark;
          if (entry.metadataWatermark === undefined || entry.metadataWatermark < watermark) {
            const binding = this.vsrSession.resumePayload(watermark);
            if (!binding)
              throw responseError(command, UNAUTHENTICATED);
            await entry.client._queueCommand(COMMAND_CODE.BindSession, binding,
              true, true, false, deadline);
            entry.metadataWatermark = watermark;
          }
          if (generation !== this.routingGeneration)
            throw responseError(command, TRANSIENT_NOT_ACCEPTED);
          sent = true;
          return await entry.client._queueCommand(command, payload, handleResponse,
            true, false, deadline, captured, attempt);
        } catch (error) {
          if (!(error instanceof ResponseError) || error instanceof VsrEvictionError ||
              error.errorCode === UNAUTHENTICATED || error.errorCode === STALE_CLIENT) {
            if (this.pollConnections.get(key) === entry)
              this.pollConnections.delete(key);
            entry.client.destroy();
            throw responseError(command, sent ? TRANSIENT_NOT_COMMITTED : TRANSIENT_NOT_ACCEPTED);
          }
          throw error;
        } finally {
          entry.busy = undefined;
          release();
        }
      } catch (error) {
        this.partitionRoutes.delete(partition.key);
        if (missingReplica || (attempt.clientId !== undefined && attempt.clientId !== this.vsrSession.clientId &&
            error instanceof ResponseError && error.errorCode !== TRANSIENT_NOT_COMMITTED))
          throw responseError(command, TRANSIENT_NOT_ACCEPTED);
        if (error instanceof ResponseError && error.commandCode === command && error.errorCode === LIFECYCLE_BUSY &&
            Date.now() + lifecycleRetryInterval < deadline) {
          await delay(lifecycleRetryInterval);
          lifecycleRetryInterval = Math.min(lifecycleRetryInterval * 2, LIFECYCLE_RETRY_MAX_INTERVAL_MS);
          attempt.frame = undefined;
          continue;
        }
        if (command === COMMAND_CODE.SendMessages && error instanceof ResponseError &&
            error.errorCode === HISTORY_UNAVAILABLE && !context && !refreshedHistory &&
            worthAnotherAttempt(deadline)) {
          refreshedHistory = true;
          const topic = partition.query.subarray(0, -PARTITION_ID_SIZE).toString('hex');
          for (const key of this.partitionRoutes.keys())
            if (key.slice(0, -PARTITION_ID_SIZE * 2) === topic)
              this.partitionRoutes.delete(key);
          captured = undefined;
          attempt.frame = undefined;
          continue;
        }
        if (error instanceof VsrResponseTimeoutError)
          throw responseError(command, attempt.frame ? TRANSIENT_NOT_COMMITTED : TRANSIENT_NOT_ACCEPTED);
        if (!(error instanceof ResponseError) ||
            (error.errorCode !== TRANSIENT_NOT_ACCEPTED && error.errorCode !== NOT_PRIMARY))
          throw error instanceof ResponseError && !(error instanceof VsrEvictionError)
            ? responseError(command, error.errorCode) : error;
        if (!worthAnotherAttempt(deadline))
          throw responseError(command, error.errorCode);
        if (error.errorCode !== NOT_PRIMARY || refusedPrimary)
          await delay(VSR_RETRY_INTERVAL_MS);
        refusedPrimary ||= error.errorCode === NOT_PRIMARY;
      }
    }
    throw responseError(command, TRANSIENT_NOT_ACCEPTED);
  }

  private async _routingControl(partition: PartitionTarget, deadline: number): Promise<CommandResponse> {
    if (!this.connection.connected || !this.isAuthenticated)
      throw responseError(COMMAND_CODE.GetPartitionRoute, TRANSIENT_NOT_ACCEPTED);
    try {
      return await this._queueCommand(COMMAND_CODE.GetPartitionRoute, partition.query,
        true, true, false, deadline);
    } catch (error) {
      if (!(error instanceof ResponseError) ||
          error.errorCode === UNAUTHENTICATED || error.errorCode === STALE_CLIENT) {
        this.connection.abort();
        this.connection.connected = false;
        this._resetSession();
      }
      throw error;
    }
  }

  private _observeMetadataReply(frame: Buffer): void {
    if (frame.length < HEADER_SIZE || peekCommand(frame) !== Command.Reply)
      return;
    const operation = readReplyOperation(frame);
    if (!isKnownOperation(operation) || operation === Operation.NonReplicated ||
        operation === Operation.SendMessages || operation === Operation.StoreConsumerOffset ||
        operation === Operation.DeleteConsumerOffset)
      return;
    const commit = frame.readBigUInt64LE(REPLY_OFFSET.commit);
    if (commit > this.metadataWatermark)
      this.metadataWatermark = commit;
  }

  private _rememberCredentials(command: number, payload: Buffer): void {
    if (command === LOGIN.code) {
      const username = readWireName(payload, 0);
      this.rememberedCredentials = {
        username: username.value, password: readWireName(payload, username.next).value
      };
    } else if (command === LOGIN_WITH_TOKEN.code) {
      this.rememberedCredentials = { token: readWireName(payload, 0).value };
    } else if ((command === COMMAND_CODE.ChangePassword || command === COMMAND_CODE.UpdateUser) &&
        this.userId !== undefined && this.rememberedCredentials &&
        'username' in this.rememberedCredentials) {
      const credentials = this.rememberedCredentials;
      const identifier = [serializeIdentifier(this.userId), serializeIdentifier(credentials.username)]
        .find((encoded) => payload.subarray(0, encoded.length).equals(encoded));
      if (!identifier)
        return;
      if (command === COMMAND_CODE.ChangePassword) {
        const currentPassword = readWireName(payload, identifier.length);
        const password = readWireName(payload, currentPassword.next).value;
        this.rememberedCredentials = { ...credentials, password };
        if ('username' in this.options.credentials && this.options.credentials.username === credentials.username)
          this.options.credentials = { ...this.options.credentials, password };
      } else if (payload[identifier.length] === 1) {
        const username = readWireName(payload, identifier.length + 1).value;
        this.rememberedCredentials = { ...credentials, username };
        if ('username' in this.options.credentials && this.options.credentials.username === credentials.username)
          this.options.credentials = { ...this.options.credentials, username };
      }
    }
  }

  private _signInCredentials(): ClientCredentials {
    return this.rememberedCredentials ?? this.options.credentials;
  }

  private _clearPollRouting(): void {
    this.routingGeneration += 1;
    this.partitionRoutes.clear();
    this.replicaEndpoints.clear();
    for (const entry of this.pollConnections.values())
      entry.client.destroy();
    this.pollConnections.clear();
  }

  /**
   * Processes queued commands sequentially.
   * Emits 'finishQueue' when all commands are processed.
   *
   */
  async _processQueue(): Promise<void> {
    if (this.busy)
      return;
    this.busy = true;
    while (this._execQueue.length > 0 && this.connection.socket.writable) {
      // While a leader move is being decided, only the roster read the move
      // itself runs goes out -- it is what decides where the client lands, and
      // it is the one command that does not follow moves. Draining the rest
      // would write them to the socket `redirect()` is about to replace, and a
      // command in flight when that happens dies with a lost-connection error
      // instead of being re-issued on the node the move lands on.
      const index = this._movePending()
        ? this._execQueue.findIndex((job) => !job.followsLeaderMoves)
        : 0;
      if (index < 0) break;
      const [next] = this._execQueue.splice(index, 1);
      if (!next) break;
      const { command, payload, handleResponse, deadline, resolve, reject } = next;
      try {
        resolve(await this._processNext(command, payload, handleResponse, deadline, next.context, next.attempt));
      } catch (err) {
        if (err instanceof LeaderMovedError && next.followsLeaderMoves)
          // Counted before the rejection is handed out, not after: the caller
          // resumes as a microtask, so this loop would otherwise write the next
          // command before the re-check it is about to start has begun.
          this.leaderMovesUndecided += 1;
        reject(err);
      }
    }
    if (this._execQueue.length > 0) {
      // The same distinction as on 'disconnected': the socket a leader move
      // replaced stops being writable, and what is still queued belongs on the
      // node being moved to.
      if (this.connection.redirecting)
        this._reissueQueue();
      else if (!this._movePending())
        this._failQueue(new Error('connection is not writable'));
      // Otherwise the move is still being decided: these commands were never
      // written, and they are drained again once it settles -- here if the
      // client stays, on the new socket if it moves.
    }
    this.busy = false;
    this._emitFinishQueue();
  }

  private _emitFinishQueue(): void {
    if (this.pendingSubmissions === 0 &&
        !this.busy &&
        this._execQueue.length === 0)
      this.emit('finishQueue');
  }

  /**
   * Processes a single command by writing it to the connection and waiting for response.
   *
   * @param command - Command code
   * @param payload - Command payload
   * @param handleResp - Whether to parse the response
   * @param deadline - When the whole request gives up, shared with the leader
   * re-checks and the re-issues after a move
   * @returns Promise resolving to the command response
   */
  _processNext(
    command: number,
    payload: Buffer,
    handleResp = true,
    deadline = Date.now() + VSR_RESPONSE_TIMEOUT_MS,
    context?: PartitionContext,
    attempt?: RoutedAttempt
  ): Promise<CommandResponse> {
    if (isLoginCommand(command))
      return this._processVsrLogin(command, payload, handleResp, deadline);
    return this._processVsr(command, payload, handleResp, deadline, context, attempt);
  }

  private async _processVsrLogin(
    command: number,
    payload: Buffer,
    handleResp: boolean,
    deadline: number
  ): Promise<CommandResponse> {
    if (this.isAuthenticated) {
      await this._processVsr(LOGOUT.code, LOGOUT.serialize(), true, deadline);
    } else {
      const resume = this.vsrSession.resumePayload(this.metadataWatermark);
      if (resume) {
        const remembered = this.rememberedCredentials;
        const sameCredentials = remembered && ('token' in remembered
          ? command === LOGIN_WITH_TOKEN.code && payload.equals(LOGIN_WITH_TOKEN.serialize(remembered))
          : command === LOGIN.code && payload.equals(LOGIN.serialize(remembered)));
        if (sameCredentials) {
          try {
            return await this._processVsr(COMMAND_CODE.BindSession, resume, handleResp, deadline);
          } catch (error) {
            if (!(error instanceof ResponseError) ||
                (error.errorCode !== UNAUTHENTICATED && error.errorCode !== STALE_CLIENT))
              throw error;
          }
        }
        this._resetSession(true);
      }
    }
    return this._processVsr(command, payload, handleResp, deadline);
  }

  private async _processVsr(
    command: number,
    payload: Buffer,
    handleResp: boolean,
    deadline: number,
    context?: PartitionContext,
    attempt?: RoutedAttempt
  ): Promise<CommandResponse> {
    let requestWritten = false;
    try {
      if (attempt?.clientId !== undefined && attempt.clientId !== this.vsrSession.clientId)
        throw responseError(command, TRANSIENT_NOT_ACCEPTED);
      if (command === COMMAND_CODE.GetPartitionRoute)
        context = { incarnation: 0n, ownerGeneration: 0n, metadataOp: this.metadataWatermark };
      const prepared = prepareVsrCommand(command, payload, this.vsrSession.bindSecret);
      // A transient retry must preserve all request identity fields.
      const frame = attempt?.frame ?? this.vsrSession.encode(prepared.command, prepared.payload, context);
      if (attempt) {
        attempt.frame = frame;
        attempt.clientId = this.vsrSession.clientId;
      }
      // Derived from the request's own budget rather than read off the clock,
      // so one request spends one budget however many times it is re-issued.
      const notAcceptedDeadline =
        deadline - VSR_RESPONSE_TIMEOUT_MS + VSR_FAILOVER_CHECK_MS;
      let lastTransientError: ResponseError | undefined;
      let parsed: CommandResponse;
      while (true) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          if (lastTransientError)
            throw lastTransientError;
          throw new VsrResponseTimeoutError(VSR_RESPONSE_TIMEOUT_MS);
        }
        const exchangeState = { written: false };
        let response: Buffer;
        try {
          response = await this._exchange(
            () => this.connection.writeFrame(frame),
            remaining,
            exchangeState
          );
        } finally {
          requestWritten ||= exchangeState.written;
        }
        this._observeMetadataReply(response);
        try {
          if (!handleResp) {
            // Routing still needs refusals when the caller decodes the frame.
            if ((this.dataConnection || attempt) && peekCommand(response) === Command.Reply &&
                readStatus(response) !== 0)
              throw responseError(command, readStatus(response));
            return response as unknown as CommandResponse;
          }
          parsed = decodeVsrResponse(response, command);
          break;
        } catch (error) {
          if (error instanceof ResponseError &&
              lastTransientError?.errorCode === TRANSIENT_NOT_COMMITTED)
            error = responseError(command, TRANSIENT_NOT_COMMITTED);
          if (!(error instanceof ResponseError) ||
              !isTransientVsrError(error.errorCode))
            throw error;
          if (command === COMMAND_CODE.GetPartitionRoute || command === COMMAND_CODE.BindSession ||
              ((this.dataConnection || attempt) &&
               (command === COMMAND_CODE.PollMessages || command === COMMAND_CODE.GetOffset)) ||
              ((this.dataConnection || attempt) && error.errorCode !== TRANSIENT_NOT_COMMITTED))
            throw error;
          if (error.errorCode === NOT_PRIMARY)
            throw new LeaderMovedError(error);
          // A later refusal cannot resolve an earlier uncertain attempt.
          lastTransientError = lastTransientError?.errorCode === TRANSIENT_NOT_COMMITTED
            ? lastTransientError : error;
          if (lastTransientError.errorCode === TRANSIENT_NOT_ACCEPTED &&
              !isLoginCommand(command) &&
              Date.now() >= notAcceptedDeadline)
            throw new LeaderMovedError(lastTransientError);
          const retryDelay = Math.min(
            VSR_RETRY_INTERVAL_MS,
            Math.max(0, deadline - Date.now())
          );
          if (retryDelay === 0)
            throw lastTransientError;
          await delay(retryDelay);
        }
      }

      if (prepared.command === COMMAND_CODE.LoginRegister ||
          prepared.command === COMMAND_CODE.LoginRegisterWithAccessToken ||
          prepared.command === COMMAND_CODE.BindSession) {
        this.vsrSession.bind(readRegisteredSession(parsed));
        this.isAuthenticated = true;
        this.userId = parsed.data.readUInt32LE(0);
      }
      this._rememberCredentials(command, payload);
      if (prepared.command === COMMAND_CODE.LogoutUser) {
        this.rememberedCredentials = undefined;
        this._resetSession(true);
      }
      // Every roster read feeds the redial candidates, whoever asked for it
      // and whatever it says: a node dies together with its address, the
      // roster is unreachable exactly when it is needed, and reading it only
      // during a login would leave the candidates stale between logins.
      if (handleResp && command === GET_CLUSTER_METADATA.code)
        this._rememberRoster(parsed);
      if (handleResp && command === COMMAND_CODE.GetPartitionRoute)
        this._rememberRoute(payload, parsed);
      return parsed;
    } catch (error) {
      // A not-admitted refusal is an answer, so the session is not in doubt
      // and the request was never applied.
      if (error instanceof LeaderMovedError)
        throw error;
      // Once bytes were handed to the socket, a local transport or decode
      // failure leaves the request outcome ambiguous. Drop the transport while
      // retaining the logical session and its bind proof.
      if (!(error instanceof ResponseError) && requestWritten)
        this._resetSession();
      if (error instanceof VsrEvictionError)
        throw error;
      if (error instanceof ResponseError)
        throw responseError(command, error.errorCode);
      throw error;
    }
  }

  private _exchange(
    write: () => void,
    timeout?: number,
    state?: ExchangeState
  ): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      let timeoutHandler: NodeJS.Timeout | undefined;
      const cleanup = () => {
        if (timeoutHandler)
          clearTimeout(timeoutHandler);
        this.connection.removeListener('error', errorCallback);
        this.connection.removeListener('disconnected', disconnectedCallback);
        this.connection.removeListener('eviction', evictionCallback);
        this.connection.removeListener('response', responseCallback);
      };
      const errorCallback = (error: unknown) => {
        cleanup();
        reject(error);
      };
      const disconnectedCallback = () => {
        cleanup();
        reject(new Error('connection closed while waiting for response'));
      };
      const evictionCallback = (error: VsrEvictionError) => {
        cleanup();
        reject(error);
      };
      const responseCallback = (response: Buffer) => {
        cleanup();
        resolve(response);
      };
      if (timeout !== undefined) {
        timeoutHandler = setTimeout(() => {
          cleanup();
          this.connection.abort();
          reject(new VsrResponseTimeoutError(timeout));
        }, timeout);
      }
      this.connection.once('error', errorCallback);
      this.connection.once('disconnected', disconnectedCallback);
      this.connection.once('eviction', evictionCallback);
      this.connection.once('response', responseCallback);
      try {
        write();
        if (state)
          state.written = true;
      } catch (error) {
        cleanup();
        reject(error);
      }
    });
  }

  // `GetClusterMetadata` is deliberately absent: the server auth-gates it,
  // so the client authenticates before reading the topology. A login dialed
  // at a backup still succeeds because the server forwards the register to
  // the primary.
  private isUnloggedCommand(command: number): boolean {
    return UNLOGGED_COMMAND_CODE.includes(command);
  }

  /**
   * Moves a freshly authenticated session to the cluster leader.
   *
   * Only the leader accepts replicated commands, and the roster read is
   * auth-gated, so the topology cannot be inspected before a login binds a
   * session. The redirect drops that session along with the socket, so the
   * login is replayed on the leader and its answer supersedes the one from the
   * node the client dialed. Leadership can move between the roster read and
   * the replay, so each freshly bound hop rechecks the roster under a bounded
   * redirect budget.
   *
   * @returns The leader's login response, or undefined when the client stays
   */
  private async _settleOnLeader(
    loginCommand: number,
    loginPayload: Buffer
  ): Promise<CommandResponse | undefined> {
    let settledResponse: CommandResponse | undefined;
    for (let redirects = 0; redirects < MAX_LEADER_REDIRECTS; redirects += 1) {
      const leader = await this._readLeaderEndpoint();
      if (!leader || this.connection.isConnectedTo(leader.host, leader.port))
        return settledResponse;
      await this.connection.redirect(leader.host, leader.port);
      settledResponse = await this.sendCommand(
        loginCommand,
        loginPayload,
        { last: false }
      );
    }
    debug(
      `leader settlement reached its ${MAX_LEADER_REDIRECTS}-hop budget, ` +
      'staying on the current node'
    );
    return settledResponse;
  }

  /**
   * Reads the cluster roster and picks the endpoint to settle on.
   *
   * Best effort: an unreadable roster, `Unauthenticated` included (the session
   * died between the login and this read), keeps the client on its current
   * node instead of failing a login that already succeeded.
   */
  private async _readLeaderEndpoint(): Promise<Endpoint | undefined> {
    // A cluster can be transiently leaderless: a restarted node cedes the
    // primaryship its stale view assigns it, and the roster reports no leader
    // until the peers' election completes. That window is roughly one heartbeat
    // timeout, so poll through it rather than settling on a replica that denies
    // every replicated command for its whole retry budget.
    const deadline = Date.now() + this.leaderlessWaitBudget;
    while (true) {
      // Reading without a session would re-enter authentication, which awaits
      // the very login this settlement runs inside of. The session can also die
      // between polls, so this holds for every pass, not just the first.
      if (!this.isAuthenticated)
        return undefined;
      try {
        // Queue the metadata fetch instead of writing directly: a bare write
        // would race an in-flight exchange and both would wake on the same
        // response event.
        const response = await this.sendCommand(
          GET_CLUSTER_METADATA.code,
          GET_CLUSTER_METADATA.serialize(),
          { last: false, followsLeaderMoves: false }
        );
        // The redial candidates are fed by `_processVsr` for every roster
        // read, leaderless ones included: a roster with no leader still names
        // where the nodes are.
        const metadata = GET_CLUSTER_METADATA.deserialize(response);
        if (metadata.nodes.length <= 1)
          return undefined;
        const leader = metadata.nodes.find(
          (node) => node.role === 'Leader' && node.status === 'Healthy'
        );
        if (leader)
          return { host: leader.ip, port: leader.endpoints.tcp };
      } catch (error) {
        debug('cluster metadata is unreadable, staying on this node', error);
        return undefined;
      }
      if (Date.now() >= deadline) {
        debug(
          'cluster metadata named no healthy leader within ' +
          `${this.leaderlessWaitBudget} ms, staying on this node`
        );
        return undefined;
      }
      await delay(this.leaderlessPollInterval);
    }
  }

  /**
   * Fails all queued commands with the given error.
   *
   * @param err - Error to reject all queued commands with
   */
  _failQueue(err: Error) {
    this._execQueue.forEach(({ reject }) => reject(err));
    this._execQueue = [];
  }

  private _resetSession(forget = false): void {
    this._clearPollRouting();
    this.clustered = undefined;
    if (!this.isAuthenticated &&
        this.userId === undefined &&
        !this.vsrSession.hasActivity)
      return;
    this.isAuthenticated = false;
    this.userId = undefined;
    if (forget)
      this.vsrSession.reset();
    this.emit('sessionReset');
  }

  hold(): () => void {
    this.pendingSubmissions += 1;
    let released = false;
    return () => {
      if (released)
        return;
      released = true;
      this.pendingSubmissions -= 1;
      this._emitFinishQueue();
    };
  }

  /**
   * Authenticates the client with the server.
   *
   * @param creds - Authentication credentials (token or password)
   * @returns True if authentication succeeded
   */
  async authenticate(creds: ClientCredentials): Promise<boolean> {
    if (this.isAuthenticated)
      return true;
    if (this.authenticationPromise)
      return this.authenticationPromise;

    this.authenticationPromise = this._authenticate(creds);
    try {
      return await this.authenticationPromise;
    } finally {
      this.authenticationPromise = undefined;
    }
  }

  private async _authenticate(creds: ClientCredentials): Promise<boolean> {
    const r = ('token' in creds) ?
      await this._authWithToken(creds) :
      await this._authWithPassword(creds);
    this.isAuthenticated = true;
    this.userId = r.userId;
    return this.isAuthenticated;
  }

  /**
   * Authenticates using username and password.
   *
   * @param creds - Password credentials
   * @returns Login response with user ID
   */
  async _authWithPassword(creds: PasswordCredentials) {
    const pl = LOGIN.serialize(creds);
    const logr = await this.sendCommand(LOGIN.code, pl, { last: false });
    return LOGIN.deserialize(logr);
  }

  /**
   * Authenticates using a token.
   *
   * @param creds - Token credentials
   * @returns Login response with user ID
   */
  async _authWithToken(creds: TokenCredentials) {
    const pl = LOGIN_WITH_TOKEN.serialize(creds);
    const logr = await this.sendCommand(
      LOGIN_WITH_TOKEN.code,
      pl,
      { last: false }
    );
    return LOGIN_WITH_TOKEN.deserialize(logr);
  }

  /**
   * Sends a ping command to the server.
   *
   * @returns Ping response
   */
  async ping() {
    const pl = PING.serialize();
    const pingR = await this.sendCommand(PING.code, pl);
    return PING.deserialize(pingR);
  }

  /**
   * Starts sending periodic heartbeat pings to keep the connection alive.
   *
   * @param interval - Heartbeat interval in milliseconds
   */
  heartbeat(interval?: number) {
    if (!interval)
      return

    this.heartbeatIntervalHandler = setInterval(async () => {
      if (this.connection.connected && !this.heartbeatInFlight) {
        this.heartbeatInFlight = true;
        debug(`sending heartbeat ping (interval: ${interval} ms)`);
        try {
          await this.ping()
          this.emit('heartbeat');
        } catch (error) {
          debug('heartbeat ping failed', error);
        } finally {
          this.heartbeatInFlight = false;
        }
      }
    }, interval);
    // A pending heartbeat must not be the reason the process stays up: a script
    // that never calls destroy() would otherwise hang on exit.
    this.heartbeatIntervalHandler.unref();
  }

  /**
   * Returns the underlying socket as a readable stream.
   *
   * @returns The connection socket
   */
  getReadStream() {
    return this.connection.socket;
  }

  /**
   * Destroys the stream and cleans up resources.
   * Stops heartbeat and destroys the connection.
   */
  destroy() {
    this.rememberedCredentials = undefined;
    this._clearPollRouting();
    if (this.heartbeatIntervalHandler)
      clearInterval(this.heartbeatIntervalHandler);
    return this.connection._destroy();
  }
};


/**
 * Creates a new RawClient instance.
 *
 * @param options - Client configuration
 * @returns RawClient instance
 */
export function getRawClient(options: ClientConfigOrString): RawClient {
  return new CommandResponseStream(options);
}

const isLoginCommand = (command: number): boolean =>
  command === COMMAND_CODE.LoginUser ||
  command === COMMAND_CODE.LoginWithAccessToken;

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

const withinDeadline = async <T>(
  pending: Promise<T>, deadline: number, expired?: () => void
): Promise<T> => {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      pending,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          expired?.();
          reject(new VsrResponseTimeoutError(VSR_RESPONSE_TIMEOUT_MS));
        }, Math.max(0, deadline - Date.now()));
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
};

const isTransientVsrError = (errorCode: number): boolean =>
  errorCode === TRANSIENT_NOT_COMMITTED ||
  errorCode === TRANSIENT_NOT_ACCEPTED || errorCode === NOT_PRIMARY;

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
    payload[position] = 1;
    payload.writeUInt32LE(0, position + 1);
  }
  const query = Buffer.concat([payload.subarray(consumerEnd, position),
    payload.subarray(position + 1, position + 1 + PARTITION_ID_SIZE)]);
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
