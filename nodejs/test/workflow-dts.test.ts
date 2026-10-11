/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import {
    InMemoryOrchestrationBackend,
    OrchestrationState,
    OrchestrationStatus,
    TestOrchestrationClient,
    TestOrchestrationWorker,
    type StartOrchestrationOptions,
    type TaskHubGrpcClient,
} from "@microsoft/durabletask-js";
import { setImmediate } from "node:timers/promises";
import { describe, expect, it, vi } from "vitest";
import { ResponseError } from "vscode-jsonrpc/node.js";
import { DtsWorkflowExecutor, type WorkflowExecutionHost } from "../src/workflow-dts.js";
import {
    defineWorkflow,
    type JsonValue,
    type WorkflowDefinition,
    type WorkflowHandle,
} from "../src/workflow.js";
import { assertWorkflowStepResult } from "../src/workflow-json.js";

const invocation = { scopeId: "scope", runId: "run", args: {} };

function json(value: unknown): JsonValue {
    assertWorkflowStepResult(value, "test");
    return value;
}

function setup(
    workflow: WorkflowHandle,
    host: WorkflowExecutionHost,
    codeVersion = "test-artifact"
) {
    const backend = new InMemoryOrchestrationBackend();
    const client = new TestOrchestrationClient(backend);
    const worker = new TestOrchestrationWorker(backend);
    const binding: Pick<TaskHubGrpcClient, "scheduleNewOrchestration" | "getOrchestrationState"> = {
        getOrchestrationState: (id, payloads) => client.getOrchestrationState(id, payloads),
        scheduleNewOrchestration: (
            name,
            input,
            options?: string | StartOrchestrationOptions,
            startAt?: Date
        ) =>
            client.scheduleNewOrchestration(
                name,
                input,
                typeof options === "string" ? options : options?.instanceId,
                startAt
            ),
    };
    return {
        client,
        binding,
        worker,
        executor: new DtsWorkflowExecutor({
            client: binding,
            worker,
            workflows: [workflow],
            codeVersion,
            executionHost: host,
        }),
    };
}

function workflow(run: WorkflowDefinition["run"]): WorkflowHandle {
    return defineWorkflow({
        meta: { name: "durable", description: "Durable test", phases: [] },
        run,
    });
}

function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((accept, decline) => {
        resolve = accept;
        reject = decline;
    });
    return { promise, resolve, reject };
}

describe("DTS scheduler RPC cancellation (mocked transport)", () => {
    it.each(
        (["lookup", "schedule", "poll"] as const).flatMap((stage) =>
            (["resolve", "reject"] as const).map((lateOutcome) => ({ stage, lateOutcome }))
        )
    )(
        "releases a stalled $stage binding and ignores its late $lateOutcome",
        async ({ stage, lateOutcome }) => {
            const lookup = deferred<OrchestrationState | undefined>();
            const schedule = deferred<string>();
            const flow = workflow(async () => "pure");
            const host: WorkflowExecutionHost = {
                agent: async () => null,
                step: async (_input, _key, producer) => producer(),
            };
            let input: unknown;
            let lookupCalls = 0;
            let scheduleCalls = 0;
            const completed = (value: string) =>
                new OrchestrationState(
                    "instance",
                    "copilot.workflow.async.v1",
                    OrchestrationStatus.COMPLETED,
                    new Date(0),
                    new Date(0),
                    JSON.stringify(input),
                    JSON.stringify({ result: value })
                );
            const getOrchestrationState = vi.fn<TaskHubGrpcClient["getOrchestrationState"]>(
                async () => {
                    lookupCalls++;
                    if (
                        (stage === "lookup" && lookupCalls === 1) ||
                        (stage === "poll" && lookupCalls === 2)
                    )
                        return lookup.promise;
                    return lookupCalls === 1 ? undefined : completed("fresh");
                }
            );
            const scheduleNewOrchestration = vi.fn(
                async (
                    _name: unknown,
                    value?: unknown,
                    _options?: string | StartOrchestrationOptions,
                    _startAt?: Date
                ) => {
                    input = value;
                    scheduleCalls++;
                    return stage === "schedule" && scheduleCalls === 1
                        ? schedule.promise
                        : "instance";
                }
            );
            const executor = new DtsWorkflowExecutor({
                client: { getOrchestrationState, scheduleNewOrchestration },
                worker: new TestOrchestrationWorker(new InMemoryOrchestrationBackend()),
                workflows: [flow],
                codeVersion: "cancellation-test",
                executionHost: host,
            });
            const controller = new AbortController();
            const waiting = executor.run(flow, invocation, controller.signal);
            let settled: "pending" | "fulfilled" | "rejected" = "pending";
            void waiting.then(
                () => {
                    settled = "fulfilled";
                },
                () => {
                    settled = "rejected";
                }
            );
            await vi.waitFor(() => {
                expect(stage === "schedule" ? scheduleCalls : lookupCalls).toBe(
                    stage === "poll" ? 2 : 1
                );
            });
            const reason = new Error("cancelled by caller");
            controller.abort(reason);
            await vi.waitFor(() => expect(settled).toBe("rejected"), { timeout: 500 });
            await expect(waiting).rejects.toBe(reason);

            const replacementLookup = deferred<OrchestrationState | undefined>();
            getOrchestrationState.mockImplementationOnce(() => replacementLookup.promise);
            const previousCalls = getOrchestrationState.mock.calls.length;
            const replacement = executor.run(flow, invocation);
            await vi.waitFor(() =>
                expect(getOrchestrationState.mock.calls.length).toBe(previousCalls + 1)
            );
            if (stage === "schedule") {
                if (lateOutcome === "resolve") schedule.resolve("late-acknowledgement");
                else schedule.reject(new Error("late scheduler failure"));
            } else {
                if (lateOutcome === "resolve") lookup.resolve(completed("late completion"));
                else lookup.reject(new Error("late scheduler failure"));
            }
            await setImmediate();
            expect(settled).toBe("rejected");
            await expect(executor.run(flow, invocation)).rejects.toThrow("already bound");
            replacementLookup.resolve(undefined);
            await expect(replacement).resolves.toEqual({ result: "fresh" });
            expect(scheduleCalls).toBe(stage === "lookup" ? 1 : 2);
        }
    );

    it("rejects completion when cancellation arrives with the scheduler reply", async () => {
        const controller = new AbortController();
        const reason = new Error("abort at completion");
        const getOrchestrationState = vi
            .fn<TaskHubGrpcClient["getOrchestrationState"]>()
            .mockResolvedValueOnce(undefined)
            .mockImplementationOnce(async () => {
                controller.abort(reason);
                return new OrchestrationState(
                    "instance",
                    "copilot.workflow.async.v1",
                    OrchestrationStatus.COMPLETED,
                    new Date(0),
                    new Date(0),
                    undefined,
                    JSON.stringify({ result: "too late" })
                );
            });
        const flow = workflow(async () => null);
        const executor = new DtsWorkflowExecutor({
            client: {
                getOrchestrationState,
                scheduleNewOrchestration: vi.fn(async () => "instance"),
            },
            worker: new TestOrchestrationWorker(new InMemoryOrchestrationBackend()),
            workflows: [flow],
            codeVersion: "completion-test",
            executionHost: {
                agent: async () => null,
                step: async (_input, _key, producer) => producer(),
            },
        });
        await expect(executor.run(flow, invocation, controller.signal)).rejects.toBe(reason);
    });

    it("does not dispatch a scheduler call for an already cancelled waiter", async () => {
        const getOrchestrationState = vi.fn<TaskHubGrpcClient["getOrchestrationState"]>();
        const scheduleNewOrchestration = vi.fn(async () => "instance");
        const flow = workflow(async () => null);
        const executor = new DtsWorkflowExecutor({
            client: { getOrchestrationState, scheduleNewOrchestration },
            worker: new TestOrchestrationWorker(new InMemoryOrchestrationBackend()),
            workflows: [flow],
            codeVersion: "pre-abort-test",
            executionHost: {
                agent: async () => null,
                step: async (_input, _key, producer) => producer(),
            },
        });
        await expect(executor.run(flow, invocation, AbortSignal.abort())).rejects.toMatchObject({
            name: "AbortError",
        });
        expect(getOrchestrationState).not.toHaveBeenCalled();
        expect(scheduleNewOrchestration).not.toHaveBeenCalled();
    });
});

describe("DTS provider with the SDK in-memory test backend (not restart proof)", () => {
    it("schedules separate effects and preserves agent and key-only step memoization", async () => {
        const agent = vi.fn(async (_input, prompt: string) => (prompt === "empty" ? null : 42));
        const step = vi.fn(async (_input, _key, producer) => producer());
        const flow = workflow(async (ctx) => {
            const first = json(await ctx.agent("same", { label: "one", model: "model" }));
            const memo = json(await ctx.agent("same", { model: "model", label: "one" }));
            const independent = json(await ctx.agent("same", { label: "two" }));
            const empty = json(await ctx.agent("empty"));
            const prepared = await ctx.step("report", () => ({ first, memo, independent, empty }));
            const again = await ctx.step("report", () => "must not execute");
            return { prepared, again };
        });
        const context = setup(flow, { agent, step });
        await context.worker.start();
        try {
            expect(await context.executor.run(flow, invocation)).toEqual({
                result: {
                    prepared: { first: 42, memo: 42, independent: 42, empty: null },
                    again: { first: 42, memo: 42, independent: 42, empty: null },
                },
            });
            expect(agent).toHaveBeenCalledTimes(3);
            expect(step).toHaveBeenCalledOnce();
            const expectedId = context.executor.getInstanceId(invocation);
            expect(
                context.executor.getInstanceId({
                    scopeId: invocation.scopeId,
                    runId: invocation.runId,
                })
            ).toBe(expectedId);
            expect(agent.mock.calls[0][0]).toMatchObject({ operationId: `${expectedId}:0` });
            expect(agent.mock.calls[1][0]).toMatchObject({ operationId: `${expectedId}:2` });
            expect(context.executor.getInstanceId({ scopeId: "other", runId: "run" })).not.toBe(
                expectedId
            );
        } finally {
            await context.worker.stop();
        }
    });

    it("records a failure and deterministically replays the author's catch path", async () => {
        const agent = vi.fn(async () => {
            throw new ResponseError(-32603, "durable storage failed", { code: "storage" });
        });
        const step = vi.fn(async (_input, _key, producer) => producer());
        const flow = workflow(async (ctx) => {
            try {
                await ctx.agent("fails");
                return null;
            } catch (error) {
                if (!(error instanceof ResponseError)) throw error;
                return await ctx.step("handled", () => ({ code: error.code, data: error.data }));
            }
        });
        const context = setup(flow, { agent, step });
        await context.worker.start();
        try {
            await expect(context.executor.run(flow, invocation)).resolves.toEqual({
                result: { code: -32603, data: { code: "storage" } },
            });
            expect(agent).toHaveBeenCalledOnce();
            expect(step).toHaveBeenCalledOnce();
        } finally {
            await context.worker.stop();
        }
    });

    it("attaches completed runs without re-executing, but rejects changed arguments or artifacts", async () => {
        const host: WorkflowExecutionHost = {
            agent: vi.fn(async () => 42),
            step: async (_input, _key, producer) => producer(),
        };
        const flow = workflow(async (ctx) => json(await ctx.agent("inspect")));
        const context = setup(flow, host);
        await context.worker.start();
        try {
            expect(await context.executor.run(flow, invocation)).toEqual({ result: 42 });
            expect(await context.executor.run(flow, invocation)).toEqual({ result: 42 });
            expect(host.agent).toHaveBeenCalledOnce();
            await expect(
                context.executor.run(flow, { ...invocation, args: { changed: true } })
            ).rejects.toMatchObject({ code: "workflow_replay_diverged" });
            const changed = workflow(async () => "changed");
            await expect(context.executor.run(changed, invocation)).rejects.toMatchObject({
                code: "workflow_replay_diverged",
            });
            const replacement = new DtsWorkflowExecutor({
                client: context.binding,
                worker: new TestOrchestrationWorker(new InMemoryOrchestrationBackend()),
                workflows: [flow],
                codeVersion: "different-artifact",
                executionHost: host,
            });
            await expect(replacement.run(flow, invocation)).rejects.toMatchObject({
                code: "workflow_replay_diverged",
            });
        } finally {
            await context.worker.stop();
        }
    });

    it("aborts a wait without terminating scheduler history", async () => {
        const host: WorkflowExecutionHost = {
            agent: async () => 42,
            step: async (_input, _key, producer) => producer(),
        };
        const flow = workflow(async (ctx) => json(await ctx.agent("inspect")));
        const context = setup(flow, host);
        const controller = new AbortController();
        const waiting = context.executor.run(flow, invocation, controller.signal);
        await vi.waitFor(async () => {
            expect(
                await context.client.getOrchestrationState(
                    context.executor.getInstanceId(invocation)
                )
            ).toBeDefined();
        });
        controller.abort();
        await expect(waiting).rejects.toMatchObject({ name: "AbortError" });
        await context.worker.start();
        try {
            expect(await context.executor.run(flow, invocation)).toEqual({ result: 42 });
        } finally {
            await context.worker.stop();
        }
    });

    it("fails the run for an unsupported producer effect even if outer user code catches", async () => {
        const host: WorkflowExecutionHost = {
            agent: vi.fn(async () => 42),
            step: async (_input, _key, producer) => producer(),
        };
        const flow = workflow(async (ctx) => {
            try {
                return await ctx.step("nested", async () => {
                    try {
                        await ctx.agent("unsupported");
                    } catch {
                        throw new Error("replacement error");
                    }
                    return null;
                });
            } catch {
                return "swallowed outside";
            }
        });
        const context = setup(flow, host);
        await context.worker.start();
        try {
            await expect(context.executor.run(flow, invocation)).rejects.toThrow(
                "SDK workflow effects are not allowed"
            );
            expect(host.agent).not.toHaveBeenCalled();
        } finally {
            await context.worker.stop();
        }
    });
});
