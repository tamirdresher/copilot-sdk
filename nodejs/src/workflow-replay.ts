/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import { AsyncLocalStorage, createHook } from "node:async_hooks";
import { createHash } from "node:crypto";
import { setImmediate } from "node:timers/promises";
import { ResponseError } from "vscode-jsonrpc/node.js";
import {
    WORKFLOW_AGENT_OPTION_KEYS,
    type JsonValue,
    type WorkflowAgentOptions,
    type WorkflowContext,
    type WorkflowDefinition,
} from "./workflow.js";
import type { WorkflowInvocation, WorkflowProgressEvent } from "./workflow-executor.js";
import { assertStrictJson, assertWorkflowResult } from "./workflow-json.js";

/**
 * A durable profile violation, not a user activity failure.
 *
 * @experimental
 */
export class WorkflowReplayError extends Error {
    constructor(
        public readonly code: "workflow_replay_diverged" | "workflow_effect_unsupported",
        message: string
    ) {
        super(message);
        this.name = "WorkflowReplayError";
    }
}

/** @internal */
export type WorkflowEffectCommand =
    | { index: number; kind: "agent"; prompt: string; options: WorkflowAgentOptions }
    | { index: number; kind: "step"; key: string; producerHash: string };

/** @internal */
export interface WorkflowParallelCommand {
    index: number;
    kind: "parallel";
    branches: string[];
}

/** @internal */
export type WorkflowCommand = WorkflowEffectCommand | WorkflowParallelCommand;

/** @internal */
export type WorkflowFailure =
    | { kind: "value"; value: JsonValue }
    | { kind: "error"; name: string; message: string; rpcCode?: number; data?: JsonValue };

/** @internal */
export type WorkflowOutcome =
    | { status: "fulfilled"; value: JsonValue }
    | { status: "rejected"; error: WorkflowFailure };

/** @internal */
export interface WorkflowHistoryEntry {
    command: WorkflowCommand;
    outcome: WorkflowOutcome;
    parallel?: WorkflowParallelHistory;
}

/** @internal */
export interface WorkflowParallelHistory {
    branches: WorkflowHistoryEntry[][];
    completionOrder: Array<{ branch: number; index: number }>;
}

/** @internal */
export type WorkflowBranchTurn =
    | { status: "command"; command: WorkflowEffectCommand }
    | { status: "completed"; result: JsonValue }
    | { status: "failed"; error: WorkflowFailure };

/** @internal */
export type WorkflowReplayTurn = (
    | { status: "command"; command: WorkflowCommand }
    | { status: "completed"; result?: JsonValue }
    | { status: "parallel"; command: WorkflowParallelCommand; branches: WorkflowBranchTurn[] }
) & { progress?: Array<Omit<WorkflowProgressEvent, "eventId">> };

/** @internal */
export function canonicalWorkflowJson(value: unknown): string {
    assertStrictJson(value, {
        code: "workflow_input_not_json",
        label: "Workflow durable input",
        allowTopLevelUndefined: false,
    });
    const canonical = (item: JsonValue): JsonValue => {
        if (Array.isArray(item)) return item.map(canonical);
        if (item !== null && typeof item === "object") {
            return Object.fromEntries(
                Object.keys(item)
                    .sort()
                    .map((key) => [key, canonical(item[key])])
            );
        }
        return item;
    };
    // The assertion disallows top-level undefined.
    if (value === undefined) throw new Error("Missing workflow durable input");
    return JSON.stringify(canonical(value));
}

/** @internal */
export function workflowHash(value: string): string {
    return createHash("sha256").update(value).digest("hex");
}

/** @internal */
export function recordWorkflowFailure(error: unknown): WorkflowFailure {
    if (!(error instanceof Error)) {
        assertStrictJson(error, {
            code: "workflow_input_not_json",
            label: "Workflow rejection",
            allowTopLevelUndefined: false,
        });
        if (error === undefined) throw new Error("Cannot record an undefined rejection");
        return { kind: "value", value: error };
    }
    const failure: WorkflowFailure = {
        kind: "error",
        name: error.name,
        message: error.message,
    };
    if (error instanceof ResponseError) {
        failure.rpcCode = error.code;
        if (error.data !== undefined) {
            canonicalWorkflowJson(error.data);
            failure.data = error.data;
        }
    }
    canonicalWorkflowJson(failure);
    return failure;
}

/** @internal */
export function restoreWorkflowFailure(failure: WorkflowFailure): unknown {
    if (failure.kind === "value") return structuredClone(failure.value);
    const error =
        failure.rpcCode === undefined
            ? new Error(failure.message)
            : new ResponseError(failure.rpcCode, failure.message, structuredClone(failure.data));
    error.name = failure.name;
    return error;
}

interface ReplayScope {
    violation?: WorkflowReplayError;
}
const replayScope = new AsyncLocalStorage<ReplayScope>();

interface ReplayFrame {
    branch?: number;
    history: WorkflowHistoryEntry[];
    cursor: number;
    inFlight: boolean;
    open: boolean;
    producing: boolean;
    frontier?: WorkflowCommand;
    producer?: () => Promise<JsonValue> | JsonValue;
    settled: boolean;
    rejected: boolean;
    result?: unknown;
    rejection?: unknown;
}

/** @internal */
export function isWorkflowFailureFatal(failure: WorkflowFailure): boolean {
    return (
        failure.kind === "error" &&
        (failure.rpcCode !== undefined ||
            [
                "ResponseError",
                "ConnectionError",
                "AbortError",
                "WorkflowReplayError",
                "TaskFailedError",
            ].includes(failure.name))
    );
}

/**
 * Replays only settled SDK operations, stopping at the first unresolved one.
 * The workflow's promise stays parked: suspension never enters its catch/finally.
 *
 * @internal
 */
export async function replayWorkflow(
    definition: WorkflowDefinition,
    invocation: WorkflowInvocation,
    history: WorkflowHistoryEntry[],
    signal: AbortSignal = new AbortController().signal,
    parallel?: WorkflowParallelHistory
): Promise<{
    turn: WorkflowReplayTurn;
    producer?: () => Promise<JsonValue>;
    parallelProducers?: Map<number, () => Promise<JsonValue>>;
}> {
    canonicalWorkflowJson(invocation);
    canonicalWorkflowJson(history);
    if (parallel) canonicalWorkflowJson(parallel);
    const scope: ReplayScope = {};
    const frames = new AsyncLocalStorage<ReplayFrame>();
    const makeFrame = (entries: WorkflowHistoryEntry[], branch?: number): ReplayFrame => ({
        history: entries,
        branch,
        cursor: 0,
        inFlight: false,
        open: true,
        producing: false,
        settled: false,
        rejected: false,
    });
    const root = makeFrame(history);
    const branchFrames: ReplayFrame[] = [];
    const progress: Array<Omit<WorkflowProgressEvent, "eventId">> = [];
    const deliveries = new Map<string, () => void>();
    let delivered = 0;

    const fail = (message: string, diverged = false): WorkflowReplayError => {
        const error = new WorkflowReplayError(
            diverged ? "workflow_replay_diverged" : "workflow_effect_unsupported",
            message
        );
        scope.violation ??= error;
        return error;
    };
    if (parallel) {
        const next = parallel.branches.map(() => 0);
        for (const entry of parallel.completionOrder) {
            const entries = parallel.branches[entry.branch];
            if (
                !entries ||
                entry.index !== next[entry.branch] ||
                entries[entry.index]?.command.index !== entry.index ||
                entries[entry.index]?.command.kind === "parallel"
            ) {
                throw fail("Invalid ordered parallel completion history", true);
            }
            next[entry.branch]++;
        }
        if (next.some((count, branch) => count !== parallel.branches[branch].length)) {
            throw fail("Missing parallel completion history", true);
        }
    }
    const parked = (): Promise<never> => new Promise(() => {});
    const unsupported = (operation: string): never => {
        throw fail(`${operation} is unsupported by the durable workflow profile`);
    };
    const current = () => frames.getStore() ?? root;
    const pump = (): void => {
        const entry = parallel?.completionOrder[delivered];
        if (!entry) return;
        const key = `${entry.branch}:${entry.index}`;
        const deliver = deliveries.get(key);
        if (!deliver) return;
        deliveries.delete(key);
        delivered++;
        deliver();
        void Promise.resolve().then(pump);
    };
    const issue = (command: WorkflowCommand): Promise<JsonValue> => {
        const frame = current();
        if (!frame.open || frame.producing) {
            throw fail(
                "SDK workflow effects are not allowed inside a step producer or after replay"
            );
        }
        if (frame.inFlight) {
            fail("Concurrent or unawaited SDK operations are unsupported; await each operation");
            return parked();
        }
        canonicalWorkflowJson(command);
        frame.inFlight = true;
        frame.cursor++;
        const recorded = frame.history[command.index];
        if (recorded) {
            if (canonicalWorkflowJson(recorded.command) !== canonicalWorkflowJson(command)) {
                fail(`Workflow operation ${command.index} changed during replay`, true);
                return parked();
            }
            const outcome = () => {
                frame.inFlight = false;
                if (recorded.outcome.status === "rejected") {
                    throw restoreWorkflowFailure(recorded.outcome.error);
                }
                return structuredClone(recorded.outcome.value);
            };
            if (frame.branch === undefined) return Promise.resolve().then(outcome);
            return new Promise((resolve, reject) => {
                deliveries.set(`${frame.branch}:${command.index}`, () => {
                    try {
                        resolve(outcome());
                    } catch (error) {
                        reject(error);
                    }
                });
                pump();
            });
        }
        frame.frontier = structuredClone(command);
        return parked();
    };
    const start = (frame: ReplayFrame, body: () => Promise<unknown> | unknown): void => {
        frames.run(frame, () => {
            Promise.resolve()
                .then(body)
                .then(
                    (value) => {
                        frame.settled = true;
                        frame.result = value;
                    },
                    (error: unknown) => {
                        frame.settled = true;
                        frame.rejected = true;
                        frame.rejection = error;
                    }
                );
        });
    };
    const emit = (kind: "phase" | "log", text: string): void => {
        const frame = current();
        if (!frame.open || frame.producing || frame.branch !== undefined) {
            return unsupported("Progress inside parallel branches or step producers");
        }
        if (typeof text !== "string" || (kind === "phase" && !text.trim())) {
            throw fail("Workflow progress must be text, with a nonempty phase title");
        }
        progress.push({ sequence: progress.length, kind, text });
    };
    const context: WorkflowContext = {
        runId: invocation.runId,
        args: structuredClone(invocation.args),
        agent: (prompt, options = {}) => {
            if (typeof prompt !== "string") throw fail("Workflow agent prompt must be a string");
            const filtered: WorkflowAgentOptions = {};
            for (const key of WORKFLOW_AGENT_OPTION_KEYS) {
                Object.assign(filtered, options[key] === undefined ? {} : { [key]: options[key] });
            }
            return issue({ index: current().cursor, kind: "agent", prompt, options: filtered });
        },
        step: (key, callback, options = {}) => {
            if (options.volatile) return unsupported("Volatile steps");
            if (typeof key !== "string" || key.length === 0 || typeof callback !== "function") {
                throw fail("Workflow step requires a nonempty key and a producer");
            }
            const command: WorkflowCommand = {
                index: current().cursor,
                kind: "step",
                key,
                producerHash: workflowHash(Function.prototype.toString.call(callback)),
            };
            const pending = issue(command);
            const frame = current();
            if (frame.frontier === undefined || command.index !== frame.frontier.index)
                return pending;
            frame.producer = callback;
            return pending;
        },
        phase: (title) => emit("phase", title),
        log: (message) => emit("log", message),
        pause: () => unsupported("pause()"),
        parallel: <TResult>(
            thunks: Array<() => Promise<TResult> | TResult>
        ): Promise<Array<TResult | null>> => {
            if (current() !== root || root.producing) return unsupported("Nested parallel()");
            if (!Array.isArray(thunks) || thunks.some((thunk) => typeof thunk !== "function")) {
                throw fail("parallel() expects an array of thunks, not promises");
            }
            if (thunks.length > 4096) throw fail("parallel() accepts at most 4096 thunks");
            const command: WorkflowParallelCommand = {
                index: root.cursor,
                kind: "parallel",
                branches: thunks.map((thunk) =>
                    workflowHash(Function.prototype.toString.call(thunk))
                ),
            };
            const pending = issue(command);
            if (root.frontier?.index === command.index && root.frontier.kind === "parallel") {
                if (parallel && parallel.branches.length !== thunks.length) {
                    throw fail("Parallel branch count changed during replay", true);
                }
                for (const [branch, thunk] of thunks.entries()) {
                    const frame = makeFrame(parallel?.branches[branch] ?? [], branch);
                    branchFrames.push(frame);
                    start(frame, thunk);
                }
            }
            return pending as Promise<Array<TResult | null>>;
        },
        pipeline: () => unsupported("pipeline()"),
        workflow: () => unsupported("Nested workflows"),
        get session() {
            return unsupported("ctx.session");
        },
        get signal() {
            if (!current().producing) return unsupported("ctx.signal outside a step producer");
            return signal;
        },
    };
    const hook = createHook({
        init(_id, type) {
            if (replayScope.getStore() === scope && type !== "PROMISE") {
                fail(`Uncontrolled async effect ${type}; put I/O inside ctx.step()`);
            }
        },
    });
    hook.enable();
    try {
        replayScope.run(scope, () => {
            start(root, () => definition.run(context));
        });
        // Drain the pure promise continuations, not arbitrary host I/O or timers.
        await setImmediate();
        if (scope.violation) throw scope.violation;
        const withProgress = progress.length ? { progress } : {};
        const captureProducer = (frame: ReplayFrame) => {
            const callback = frame.producer;
            return callback
                ? async () => {
                      frame.producing = true;
                      try {
                          const value = await frames.run(frame, callback);
                          if (scope.violation) throw scope.violation;
                          return value;
                      } catch (error) {
                          throw scope.violation ?? error;
                      } finally {
                          frame.producing = false;
                      }
                  }
                : undefined;
        };
        if (root.frontier) {
            if (root.settled)
                throw fail("Workflow settled without awaiting its pending SDK operation");
            if (root.frontier.kind === "parallel") {
                if (parallel && delivered !== parallel.completionOrder.length) {
                    throw fail("Parallel completion history could not be replayed", true);
                }
                const producers = new Map<number, () => Promise<JsonValue>>();
                const branches: WorkflowBranchTurn[] = branchFrames.map((frame, branch) => {
                    if (frame.frontier) {
                        if (frame.settled)
                            throw fail("Parallel branch did not await its pending operation");
                        if (frame.frontier.kind === "parallel")
                            return unsupported("Nested parallel()");
                        const producer = captureProducer(frame);
                        if (producer) producers.set(branch, producer);
                        return { status: "command", command: frame.frontier };
                    }
                    if (!frame.settled) throw fail("Uncontrolled promise in parallel branch");
                    if (frame.cursor !== frame.history.length)
                        throw fail("Parallel branch omitted recorded operations", true);
                    if (frame.rejected) {
                        const failure = recordWorkflowFailure(frame.rejection);
                        return isWorkflowFailureFatal(failure)
                            ? { status: "failed", error: failure }
                            : { status: "completed", result: null };
                    }
                    try {
                        assertStrictJson(frame.result, {
                            code: "workflow_result_not_json",
                            label: "Parallel result",
                            allowTopLevelUndefined: false,
                        });
                    } catch {
                        throw fail("Parallel branch result must be lossless JSON");
                    }
                    if (frame.result === undefined)
                        throw fail("Parallel branch returned undefined");
                    return { status: "completed", result: frame.result };
                });
                return {
                    turn: { status: "parallel", command: root.frontier, branches, ...withProgress },
                    parallelProducers: producers,
                };
            }
            if (parallel) throw fail("Parallel operation changed during replay", true);
            const callback = captureProducer(root);
            return {
                turn: { status: "command", command: root.frontier, ...withProgress },
                ...(callback ? { producer: callback } : {}),
            };
        }
        if (parallel) throw fail("Parallel operation omitted during replay", true);
        if (!root.settled) throw fail("Uncontrolled promise: workflow must await an SDK operation");
        if (root.cursor !== history.length)
            throw fail("Workflow omitted recorded operations", true);
        if (root.rejected) throw root.rejection;
        assertWorkflowResult(root.result);
        return {
            turn:
                root.result === undefined
                    ? { status: "completed", ...withProgress }
                    : { status: "completed", result: root.result, ...withProgress },
        };
    } finally {
        for (const frame of [root, ...branchFrames]) frame.open = false;
        hook.disable();
    }
}
