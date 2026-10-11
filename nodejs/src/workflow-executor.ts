/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import type { JsonValue, WorkflowAgentOptions, WorkflowDefinition } from "./workflow.js";

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
