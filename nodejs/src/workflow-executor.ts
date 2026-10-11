/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import type { JsonValue, WorkflowAgentOptions, WorkflowDefinition } from "./workflow.js";

/** @internal */
export async function awaitWorkflowOperation<TResult>(
    operation: () => Promise<TResult>,
    signal: AbortSignal
): Promise<TResult> {
    let rejectAbort: ((reason?: unknown) => void) | undefined;
    const aborted = new Promise<never>((_resolve, reject) => {
        rejectAbort = reject;
    });
    const onAbort = () => rejectAbort?.(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    try {
        // Register/check before dispatch. The race also consumes late RPC rejections.
        signal.throwIfAborted();
        const result = await Promise.race([
            Promise.resolve().then(() => {
                signal.throwIfAborted();
                return operation();
            }),
            aborted,
        ]);
        signal.throwIfAborted();
        return result;
    } finally {
        signal.removeEventListener("abort", onAbort);
    }
}

/**
 * Stable, non-secret identity and input supplied by a workflow's owning host.
 *
 * @experimental
 */
export interface WorkflowInvocation {
    /** Session or service scope owning this run. */
    scopeId: string;
    runId: string;
    args: JsonValue;
}

/**
 * A replay-stable activity locator. Hosts can use it as an idempotency key.
 *
 * @experimental
 */
export interface WorkflowActivityInvocation extends WorkflowInvocation {
    operationId: string;
}

/**
 * Replay-stable progress metadata. Sinks must deduplicate redelivery by eventId.
 *
 * @experimental
 */
export interface WorkflowProgressEvent {
    eventId: string;
    sequence: number;
    kind: "phase" | "log";
    text: string;
}

/**
 * Executes effects, independently of the provider that stores orchestration history.
 *
 * Functions, sessions, credentials, and current execution tokens stay on this
 * live boundary. A remote step host must load the same pinned workflow artifact
 * and reconstruct the producer; it must not serialize the supplied closure.
 *
 * @experimental
 */
export interface WorkflowExecutionHost {
    agent(
        invocation: WorkflowActivityInvocation,
        prompt: string,
        options: WorkflowAgentOptions
    ): Promise<JsonValue>;
    step(
        invocation: WorkflowActivityInvocation,
        key: string,
        producer: () => Promise<JsonValue> | JsonValue
    ): Promise<JsonValue>;
    /** Required when the workflow emits phases or logs; never a silent no-op. */
    progress?(invocation: WorkflowInvocation, event: WorkflowProgressEvent): Promise<void>;
}

/**
 * Host-owned workflow execution binding. It is never workflow metadata.
 *
 * @experimental
 */
export interface WorkflowExecutor {
    execute(
        definition: WorkflowDefinition,
        invocation: WorkflowInvocation,
        host: WorkflowExecutionHost,
        signal: AbortSignal
    ): Promise<{ result?: JsonValue }>;
}
