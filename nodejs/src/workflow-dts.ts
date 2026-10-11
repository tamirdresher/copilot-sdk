/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import {
    OrchestrationStatus,
    Task,
    whenAny,
    type OrchestrationContext,
    type TaskHubGrpcClient,
    type TaskHubGrpcWorker,
} from "@microsoft/durabletask-js";
import { setTimeout } from "node:timers/promises";
import {
    getWorkflowDefinition,
    type JsonValue,
    type WorkflowDefinition,
    type WorkflowHandle,
} from "./workflow.js";
import {
    awaitWorkflowOperation,
    type WorkflowActivityInvocation,
    type WorkflowExecutionHost,
    type WorkflowExecutor,
    type WorkflowInvocation,
    type WorkflowProgressEvent,
} from "./workflow-executor.js";
import { assertWorkflowResult, assertWorkflowStepResult } from "./workflow-json.js";
import {
    canonicalWorkflowJson,
    isWorkflowFailureFatal,
    recordWorkflowFailure,
    replayWorkflow,
    workflowHash,
    WorkflowReplayError,
    type WorkflowEffectCommand,
    type WorkflowHistoryEntry,
    type WorkflowOutcome,
    type WorkflowReplayTurn,
    type WorkflowParallelHistory,
} from "./workflow-replay.js";

export { WorkflowReplayError } from "./workflow-replay.js";
export type {
    WorkflowActivityInvocation,
    WorkflowExecutionHost,
    WorkflowExecutor,
    WorkflowInvocation,
    WorkflowProgressEvent,
} from "./workflow-executor.js";

const ORCHESTRATOR = "copilot.workflow.async.v2";
const ADVANCE = "copilot.workflow.advance.v2";
const EXECUTE_AGENT = "copilot.workflow.agent.v2";
const EXECUTE_STEP = "copilot.workflow.step.v2";
const PROGRESS = "copilot.workflow.progress.v2";

interface PinnedInvocation extends WorkflowInvocation {
    name: string;
    definitionHash: string;
}

interface ReplayRequest {
    input: PinnedInvocation;
    history: WorkflowHistoryEntry[];
    parallel?: WorkflowParallelHistory;
}

interface ActivityRequest extends ReplayRequest {
    command: WorkflowEffectCommand;
    branch?: number;
    parallelIndex?: number;
}

interface ProgressRequest {
    input: PinnedInvocation;
    event: Omit<WorkflowProgressEvent, "eventId">;
}

/**
 * Runtime-owned bindings for the experimental DTS provider.
 *
 * The runtime supplies an already configured client/worker; workflow authors do
 * not supply endpoints, credentials, or a customer-managed scheduler resource.
 *
 * @experimental
 */
export interface DtsWorkflowExecutorOptions {
    client: Pick<TaskHubGrpcClient, "getOrchestrationState" | "scheduleNewOrchestration">;
    worker: Pick<TaskHubGrpcWorker, "addNamedOrchestrator" | "addNamedActivity">;
    workflows: WorkflowHandle[];
    /** Immutable artifact identity covering workflow code and its dependencies. */
    codeVersion: string;
    /** Required for headless execution and cold-worker recovery. */
    executionHost?: WorkflowExecutionHost;
}

function memoIdentity(command: WorkflowEffectCommand): string {
    return command.kind === "step"
        ? `step:${command.key}`
        : `agent:${canonicalWorkflowJson({ prompt: command.prompt, options: command.options })}`;
}

type DriverReply = WorkflowReplayTurn | WorkflowOutcome | Task<WorkflowOutcome> | undefined;

function replayTurn(reply: DriverReply): WorkflowReplayTurn {
    if (
        !reply ||
        reply instanceof Task ||
        reply.status === "fulfilled" ||
        reply.status === "rejected"
    ) {
        throw new WorkflowReplayError("workflow_replay_diverged", "Invalid replay activity reply");
    }
    return reply;
}

function memoizedEffects(history: WorkflowHistoryEntry[]): Map<string, WorkflowOutcome> {
    const memo = new Map<string, WorkflowOutcome>();
    for (const entry of history) {
        if (entry.parallel) {
            for (const branch of entry.parallel.branches) {
                for (const [key, value] of memoizedEffects(branch)) memo.set(key, value);
            }
        }
        if (entry.command.kind !== "parallel" && entry.outcome.status === "fulfilled") {
            memo.set(memoIdentity(entry.command), entry.outcome);
        }
    }
    return memo;
}

async function* executeParallel(
    context: OrchestrationContext,
    input: PinnedInvocation,
    history: WorkflowHistoryEntry[],
    initial: Extract<WorkflowReplayTurn, { status: "parallel" }>
): AsyncGenerator<Task<unknown>, WorkflowHistoryEntry, DriverReply> {
    const parallel: WorkflowParallelHistory = {
        branches: initial.branches.map(() => []),
        completionOrder: [],
    };
    const memo = memoizedEffects(history);
    let turn = initial;
    while (true) {
        const fatal = turn.branches.find((branch) => branch.status === "failed");
        if (fatal?.status === "failed") {
            return {
                command: initial.command,
                outcome: { status: "rejected", error: fatal.error },
                parallel,
            };
        }
        if (turn.branches.every((branch) => branch.status === "completed")) {
            return {
                command: initial.command,
                outcome: {
                    status: "fulfilled",
                    value: turn.branches.map((branch) => {
                        if (branch.status !== "completed")
                            throw new Error("Parallel result is incomplete");
                        return branch.result;
                    }),
                },
                parallel,
            };
        }
        const pending = new Map<
            string,
            {
                task: Task<WorkflowOutcome>;
                targets: Array<{ branch: number; command: WorkflowEffectCommand }>;
            }
        >();
        const record = (
            branch: number,
            command: WorkflowEffectCommand,
            outcome: WorkflowOutcome
        ) => {
            parallel.branches[branch].push({ command, outcome });
            parallel.completionOrder.push({ branch, index: command.index });
        };
        for (const [branch, state] of turn.branches.entries()) {
            if (state.status !== "command") continue;
            const key = memoIdentity(state.command);
            const hit = memo.get(key);
            if (hit) {
                record(branch, state.command, hit);
                continue;
            }
            const shared = pending.get(key);
            if (shared) {
                shared.targets.push({ branch, command: state.command });
                continue;
            }
            pending.set(key, {
                task: context.callActivity<ActivityRequest, WorkflowOutcome>(
                    state.command.kind === "agent" ? EXECUTE_AGENT : EXECUTE_STEP,
                    {
                        input,
                        history,
                        command: state.command,
                        branch,
                        parallelIndex: initial.command.index,
                        parallel: structuredClone(parallel),
                    }
                ),
                targets: [{ branch, command: state.command }],
            });
        }
        let failure: WorkflowOutcome | undefined;
        let taskFailure: Error | undefined;
        // All distinct effects are scheduled before the first wait. Drain admitted
        // tasks on failure instead of silently orphaning another parallel branch.
        while (pending.size > 0) {
            const winner = yield whenAny([...pending.values()].map(({ task }) => task));
            if (!(winner instanceof Task)) throw new Error("Invalid parallel completion task");
            const pair = [...pending.entries()].find(([, item]) => item.task === winner);
            if (!pair) throw new Error("Unknown parallel completion task");
            const [key, item] = pair;
            pending.delete(key);
            let outcome: WorkflowOutcome;
            if (winner.isFailed) {
                taskFailure ??= winner.getException();
                outcome = {
                    status: "rejected",
                    error: recordWorkflowFailure(winner.getException()),
                };
            } else {
                outcome = winner.getResult();
            }
            if (outcome.status === "fulfilled") memo.set(key, outcome);
            else if (isWorkflowFailureFatal(outcome.error)) failure ??= outcome;
            for (const target of item.targets) record(target.branch, target.command, outcome);
        }
        // Provider/profile failures must not become catchable branch outcomes.
        if (taskFailure) throw taskFailure;
        if (failure) return { command: initial.command, outcome: failure, parallel };
        const reply = replayTurn(
            yield context.callActivity<ReplayRequest, WorkflowReplayTurn>(ADVANCE, {
                input,
                history,
                parallel,
            })
        );
        if (
            reply.status !== "parallel" ||
            canonicalWorkflowJson(reply.command) !== canonicalWorkflowJson(initial.command)
        ) {
            throw new WorkflowReplayError(
                "workflow_replay_diverged",
                "Parallel definition changed"
            );
        }
        turn = reply;
    }
}

async function* orchestrate(
    context: OrchestrationContext,
    input: PinnedInvocation
): AsyncGenerator<Task<unknown>, { result?: JsonValue }, DriverReply> {
    const history: WorkflowHistoryEntry[] = [];
    const progress: Array<Omit<WorkflowProgressEvent, "eventId">> = [];
    while (true) {
        const turn = replayTurn(
            yield context.callActivity<ReplayRequest, WorkflowReplayTurn>(ADVANCE, {
                input,
                history,
            })
        );
        const lines = turn.progress ?? [];
        if (
            lines.length < progress.length ||
            canonicalWorkflowJson(lines.slice(0, progress.length)) !==
                canonicalWorkflowJson(progress)
        ) {
            throw new WorkflowReplayError(
                "workflow_replay_diverged",
                "Workflow progress changed during replay"
            );
        }
        for (const line of lines.slice(progress.length)) {
            yield context.callActivity<ProgressRequest, void>(PROGRESS, { input, event: line });
            progress.push(line);
        }
        if (turn.status === "completed") {
            return turn.result === undefined ? {} : { result: turn.result };
        }
        if (turn.status === "parallel") {
            if (turn.command.index !== history.length)
                throw new Error("Invalid parallel operation order");
            history.push(yield* executeParallel(context, input, history, turn));
            continue;
        }
        if (turn.status !== "command" || turn.command.index !== history.length) {
            throw new WorkflowReplayError(
                "workflow_replay_diverged",
                "Invalid replay command order"
            );
        }
        if (turn.command.kind === "parallel") throw new Error("Missing parallel branch discovery");
        const previous = memoizedEffects(history).get(memoIdentity(turn.command));
        if (previous) {
            history.push({ command: turn.command, outcome: previous });
            continue;
        }
        const outcome = yield context.callActivity<ActivityRequest, WorkflowOutcome>(
            turn.command.kind === "agent" ? EXECUTE_AGENT : EXECUTE_STEP,
            { input, history, command: turn.command }
        );
        if (
            !outcome ||
            outcome instanceof Task ||
            (outcome.status !== "fulfilled" && outcome.status !== "rejected")
        ) {
            throw new WorkflowReplayError("workflow_replay_diverged", "Invalid activity outcome");
        }
        history.push({ command: turn.command, outcome });
    }
}

/**
 * Executes existing async workflow bodies using real DTS-scheduled activities.
 *
 * Supports sequential effects, top-level parallel branches, and host progress. Pure body
 * code is replayed in an Advance activity, outside the deterministic DTS
 * orchestrator. Step producers are recreated by replaying the pinned prefix in
 * the ExecuteStep activity. See the workflow documentation for restrictions.
 *
 * The owner starts/stops the supplied worker and client. Stopping a worker or
 * aborting a wait does not delete or terminate scheduler history.
 *
 * @experimental
 */
export class DtsWorkflowExecutor implements WorkflowExecutor {
    private readonly definitions = new Map<string, WorkflowDefinition>();
    private readonly bindings = new Map<
        string,
        { host: WorkflowExecutionHost; signal: AbortSignal }
    >();

    constructor(private readonly options: DtsWorkflowExecutorOptions) {
        if (!options.codeVersion.trim()) throw new Error("DTS requires an immutable codeVersion");
        for (const handle of options.workflows) {
            const definition = getWorkflowDefinition(handle);
            if (this.definitions.has(definition.meta.name)) {
                throw new Error(`Duplicate DTS workflow "${definition.meta.name}"`);
            }
            if (Object.keys(definition.meta.limits ?? {}).length > 0) {
                throw new WorkflowReplayError(
                    "workflow_effect_unsupported",
                    "DTS resource-limit accounting requires native runtime integration"
                );
            }
            this.definitions.set(definition.meta.name, definition);
        }
        options.worker.addNamedOrchestrator(ORCHESTRATOR, orchestrate);
        options.worker.addNamedActivity(ADVANCE, async (_context, request: ReplayRequest) => {
            const definition = this.definitionFor(request.input);
            return (
                await replayWorkflow(
                    definition,
                    request.input,
                    request.history,
                    undefined,
                    request.parallel
                )
            ).turn;
        });
        options.worker.addNamedActivity(PROGRESS, async (_context, request: ProgressRequest) => {
            this.definitionFor(request.input);
            const host = this.hostFor(request.input);
            if (!host.progress)
                throw new Error("Workflow phases/logs require an executionHost.progress sink");
            await host.progress(this.invocation(request.input), {
                ...request.event,
                eventId: `${this.getInstanceId(request.input)}:progress:${request.event.sequence}`,
            });
        });
        options.worker.addNamedActivity(
            EXECUTE_AGENT,
            async (_context, request: ActivityRequest) => {
                this.definitionFor(request.input);
                if (request.command.kind !== "agent") throw new Error("Expected an agent activity");
                const command = request.command;
                const host = this.hostFor(request.input);
                return this.outcome(async () => {
                    const value = await host.agent(
                        this.activityInvocation(request),
                        command.prompt,
                        command.options
                    );
                    assertWorkflowStepResult(value, "agent");
                    return value;
                });
            }
        );
        options.worker.addNamedActivity(
            EXECUTE_STEP,
            async (_context, request: ActivityRequest) => {
                const definition = this.definitionFor(request.input);
                if (request.command.kind !== "step") throw new Error("Expected a step activity");
                const replay = await replayWorkflow(
                    definition,
                    request.input,
                    request.history,
                    this.bindings.get(this.getInstanceId(request.input))?.signal,
                    request.parallel
                );
                const command =
                    replay.turn.status === "parallel" && request.branch !== undefined
                        ? replay.turn.branches[request.branch]
                        : replay.turn;
                const producer =
                    request.branch === undefined
                        ? replay.producer
                        : replay.parallelProducers?.get(request.branch);
                if (
                    command?.status !== "command" ||
                    !producer ||
                    canonicalWorkflowJson(command.command) !==
                        canonicalWorkflowJson(request.command)
                ) {
                    throw new WorkflowReplayError(
                        "workflow_replay_diverged",
                        "Step producer locator changed during replay"
                    );
                }
                const host = this.hostFor(request.input);
                const key = request.command.key;
                return this.outcome(async () => {
                    const value = await host.step(this.activityInvocation(request), key, producer);
                    assertWorkflowStepResult(value, key);
                    return value;
                });
            }
        );
    }

    /** Stable scheduler instance identity, independent of worker and execution attempt. */
    getInstanceId(invocation: Pick<WorkflowInvocation, "scopeId" | "runId">): string {
        if (!invocation.scopeId || !invocation.runId) {
            throw new Error("Workflow scopeId and runId must not be empty");
        }
        return `copilot-${workflowHash(
            canonicalWorkflowJson({ scopeId: invocation.scopeId, runId: invocation.runId })
        )}`;
    }

    /** Start or attach to the same headless run using the runtime-provided host. */
    async run(
        workflow: WorkflowHandle,
        invocation: WorkflowInvocation,
        signal: AbortSignal = new AbortController().signal
    ): Promise<{ result?: JsonValue }> {
        if (!this.options.executionHost)
            throw new Error("A headless workflow needs an executionHost");
        return this.execute(
            getWorkflowDefinition(workflow),
            invocation,
            this.options.executionHost,
            signal
        );
    }

    /** Implements the local contribution binding used by joinSession. */
    async execute(
        definition: WorkflowDefinition,
        invocation: WorkflowInvocation,
        host: WorkflowExecutionHost,
        signal: AbortSignal
    ): Promise<{ result?: JsonValue }> {
        signal.throwIfAborted();
        const input: PinnedInvocation = {
            ...structuredClone(invocation),
            name: definition.meta.name,
            definitionHash: this.definitionHash(definition),
        };
        this.definitionFor(input);
        canonicalWorkflowJson(input);
        const instanceId = this.getInstanceId({
            scopeId: invocation.scopeId,
            runId: invocation.runId,
        });
        if (this.bindings.has(instanceId)) throw new Error("Workflow execution is already bound");
        const binding = { host, signal };
        this.bindings.set(instanceId, binding);
        try {
            const existing = await awaitWorkflowOperation(
                () => this.options.client.getOrchestrationState(instanceId, true),
                signal
            );
            if (existing) {
                if (
                    existing.name !== ORCHESTRATOR ||
                    existing.serializedInput === undefined ||
                    canonicalWorkflowJson(JSON.parse(existing.serializedInput)) !==
                        canonicalWorkflowJson(input)
                ) {
                    throw new WorkflowReplayError(
                        "workflow_replay_diverged",
                        "Workflow definition, runtime version, or input changed for this run"
                    );
                }
            } else {
                await awaitWorkflowOperation(
                    () =>
                        this.options.client.scheduleNewOrchestration(ORCHESTRATOR, input, {
                            instanceId,
                        }),
                    signal
                );
            }
            while (true) {
                signal.throwIfAborted();
                const state = await awaitWorkflowOperation(
                    () => this.options.client.getOrchestrationState(instanceId, true),
                    signal
                );
                if (!state) throw new Error("Scheduled workflow instance is missing");
                state.raiseIfFailed();
                if (state.runtimeStatus === OrchestrationStatus.TERMINATED) {
                    throw new Error("DTS workflow was terminated");
                }
                if (state.runtimeStatus === OrchestrationStatus.COMPLETED) {
                    const output: unknown = JSON.parse(state.serializedOutput ?? "");
                    if (typeof output !== "object" || output === null || Array.isArray(output)) {
                        throw new Error("Invalid DTS workflow output");
                    }
                    if (Object.keys(output).some((key) => key !== "result")) {
                        throw new Error("Unexpected fields in DTS workflow output");
                    }
                    const result = "result" in output ? output.result : undefined;
                    assertWorkflowResult(result);
                    signal.throwIfAborted();
                    return result === undefined ? {} : { result };
                }
                await setTimeout(250, undefined, { signal });
            }
        } finally {
            if (this.bindings.get(instanceId) === binding) this.bindings.delete(instanceId);
        }
    }

    private definitionHash(definition: WorkflowDefinition): string {
        return workflowHash(
            canonicalWorkflowJson({
                protocol: ORCHESTRATOR,
                durableTaskVersion: "0.4.0",
                nodeVersion: process.versions.node,
                codeVersion: this.options.codeVersion,
                meta: JSON.parse(JSON.stringify(definition.meta)),
                run: Function.prototype.toString.call(definition.run),
            })
        );
    }

    private definitionFor(input: PinnedInvocation): WorkflowDefinition {
        const definition = this.definitions.get(input.name);
        if (!definition || this.definitionHash(definition) !== input.definitionHash) {
            throw new WorkflowReplayError(
                "workflow_replay_diverged",
                "Worker must load the same pinned workflow artifact and runtime"
            );
        }
        return definition;
    }

    private hostFor(input: PinnedInvocation): WorkflowExecutionHost {
        const instanceId = this.getInstanceId({ scopeId: input.scopeId, runId: input.runId });
        const host = this.bindings.get(instanceId)?.host ?? this.options.executionHost;
        if (!host) throw new Error("No live execution host is bound to this workflow");
        return host;
    }

    private activityInvocation(request: ActivityRequest): WorkflowActivityInvocation {
        return {
            ...this.invocation(request.input),
            operationId: `${this.getInstanceId({
                scopeId: request.input.scopeId,
                runId: request.input.runId,
            })}:${
                request.branch === undefined
                    ? request.command.index
                    : `${request.parallelIndex}/${request.branch}/${request.command.index}`
            }`,
        };
    }

    private invocation(input: PinnedInvocation): WorkflowInvocation {
        return { scopeId: input.scopeId, runId: input.runId, args: structuredClone(input.args) };
    }

    private async outcome(producer: () => Promise<JsonValue>): Promise<WorkflowOutcome> {
        try {
            return { status: "fulfilled", value: await producer() };
        } catch (error) {
            if (error instanceof WorkflowReplayError) throw error;
            return { status: "rejected", error: recordWorkflowFailure(error) };
        }
    }
}
