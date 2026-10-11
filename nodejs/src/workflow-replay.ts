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
import type { WorkflowInvocation } from "./workflow-executor.js";
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
export type WorkflowCommand =
    | { index: number; kind: "agent"; prompt: string; options: WorkflowAgentOptions }
    | { index: number; kind: "step"; key: string; producerHash: string };

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
}

/** @internal */
export type WorkflowReplayTurn =
    | { status: "command"; command: WorkflowCommand }
    | { status: "completed"; result?: JsonValue };

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
    signal: AbortSignal = new AbortController().signal
): Promise<{
    turn: WorkflowReplayTurn;
    producer?: () => Promise<JsonValue>;
}> {
    canonicalWorkflowJson(invocation);
    canonicalWorkflowJson(history);
    const scope: ReplayScope = {};
    let cursor = 0;
    let inFlight = false;
    let open = true;
    let producing = false;
    let frontier: WorkflowCommand | undefined;
    let producer: (() => Promise<JsonValue> | JsonValue) | undefined;
    let settled = false;
    let rejected = false;
    let result: unknown;
    let rejection: unknown;

    const fail = (message: string, diverged = false): WorkflowReplayError => {
        const error = new WorkflowReplayError(
            diverged ? "workflow_replay_diverged" : "workflow_effect_unsupported",
            message
        );
        scope.violation ??= error;
        return error;
    };
    const parked = (): Promise<never> => new Promise(() => {});
    const unsupported = (operation: string): never => {
        throw fail(`${operation} is unsupported by the sequential durable workflow profile`);
    };
    const issue = (command: WorkflowCommand): Promise<JsonValue> => {
        if (!open || producing) {
            throw fail(
                "SDK workflow effects are not allowed inside a step producer or after replay"
            );
        }
        if (inFlight) {
            fail("Concurrent or unawaited SDK operations are unsupported; await each operation");
            return parked();
        }
        canonicalWorkflowJson(command);
        inFlight = true;
        cursor++;
        const recorded = history[command.index];
        if (recorded) {
            if (canonicalWorkflowJson(recorded.command) !== canonicalWorkflowJson(command)) {
                fail(`Workflow operation ${command.index} changed during replay`, true);
                return parked();
            }
            return Promise.resolve().then(() => {
                inFlight = false;
                if (recorded.outcome.status === "rejected") {
                    throw restoreWorkflowFailure(recorded.outcome.error);
                }
                return structuredClone(recorded.outcome.value);
            });
        }
        frontier = structuredClone(command);
        return parked();
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
            return issue({ index: cursor, kind: "agent", prompt, options: filtered });
        },
        step: (key, callback, options = {}) => {
            if (options.volatile) return unsupported("Volatile steps");
            if (typeof key !== "string" || key.length === 0 || typeof callback !== "function") {
                throw fail("Workflow step requires a nonempty key and a producer");
            }
            const command: WorkflowCommand = {
                index: cursor,
                kind: "step",
                key,
                producerHash: workflowHash(Function.prototype.toString.call(callback)),
            };
            const pending = issue(command);
            if (frontier === undefined || command.index !== frontier.index) return pending;
            producer = callback;
            return pending;
        },
        phase: () => unsupported("phase()"),
        log: () => unsupported("log()"),
        pause: () => unsupported("pause()"),
        parallel: () => unsupported("parallel()"),
        pipeline: () => unsupported("pipeline()"),
        workflow: () => unsupported("Nested workflows"),
        get session() {
            return unsupported("ctx.session");
        },
        get signal() {
            if (!producing) return unsupported("ctx.signal outside a step producer");
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
            Promise.resolve()
                .then(() => definition.run(context))
                .then(
                    (value) => {
                        settled = true;
                        result = value;
                    },
                    (error: unknown) => {
                        settled = true;
                        rejected = true;
                        rejection = error;
                    }
                );
        });
        // Drain the pure promise continuations, not arbitrary host I/O or timers.
        await setImmediate();
        if (scope.violation) throw scope.violation;
        if (frontier) {
            if (settled) throw fail("Workflow settled without awaiting its pending SDK operation");
            const callback = producer;
            return {
                turn: { status: "command", command: frontier },
                ...(callback
                    ? {
                          producer: async () => {
                              producing = true;
                              try {
                                  const value = await callback();
                                  if (scope.violation) throw scope.violation;
                                  return value;
                              } catch (error) {
                                  throw scope.violation ?? error;
                              } finally {
                                  producing = false;
                              }
                          },
                      }
                    : {}),
            };
        }
        if (!settled) throw fail("Uncontrolled promise: workflow must await an SDK operation");
        if (cursor !== history.length) throw fail("Workflow omitted recorded operations", true);
        if (rejected) throw rejection;
        assertWorkflowResult(result);
        return {
            turn: result === undefined ? { status: "completed" } : { status: "completed", result },
        };
    } finally {
        open = false;
        hook.disable();
    }
}
