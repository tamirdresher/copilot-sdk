/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import {
    InMemoryOrchestrationBackend,
    TestOrchestrationClient,
    TestOrchestrationWorker,
    type StartOrchestrationOptions,
    type TaskHubGrpcClient,
} from "@microsoft/durabletask-js";
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
