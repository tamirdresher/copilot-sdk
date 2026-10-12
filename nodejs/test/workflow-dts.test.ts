/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import {
    InMemoryOrchestrationBackend,
    ActivityContext,
    OrchestrationState,
    OrchestrationStatus,
    TestOrchestrationClient,
    TestOrchestrationWorker,
    type StartOrchestrationOptions,
    type TaskHubGrpcClient,
    type TaskHubGrpcWorker,
} from "@microsoft/durabletask-js";
import { setImmediate } from "node:timers/promises";
import { describe, expect, it, vi } from "vitest";
import { ResponseError } from "vscode-jsonrpc/node.js";
import {
    DtsWorkflowExecutor,
    DtsWorkflowContinuationExecutor,
    type WorkflowExecutionHost,
    type WorkflowInvocation,
    type WorkflowProgressEvent,
} from "../src/workflow-dts.js";
import {
    defineWorkflow,
    getWorkflowDefinition,
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

describe("DTS new-native-carrier handoff (mock scheduler, no native recovery claim)", () => {
    async function exerciseProgressHost(mutates: boolean): Promise<void> {
        const flow = workflow(async (ctx) => {
            const first = json(await ctx.agent("first"));
            const second = json(await ctx.agent("second"));
            return { first, second };
        });
        const definition = getWorkflowDefinition(flow);
        const original = { scopeId: "scope", runId: "old-native", args: { input: 1 } };
        const current = { ...original, runId: "new-native", args: { ...original.args } };
        const activities = new Map<string, Parameters<TaskHubGrpcWorker["addNamedActivity"]>[1]>();
        const worker = new TestOrchestrationWorker(new InMemoryOrchestrationBackend());
        vi.spyOn(worker, "addNamedActivity").mockImplementation((name, body) => {
            activities.set(name, body);
            return name;
        });
        const invoke = async (name: string, input: unknown) => {
            const activity = activities.get(name);
            if (!activity) throw new Error(`Missing activity ${name}`);
            return await activity(new ActivityContext("instance", 1), input);
        };
        let saved: unknown;
        let firstOutcome: unknown;
        let calls = 0;
        const controller = new AbortController();
        const oldHost: WorkflowExecutionHost = {
            agent: vi.fn(async () => 7),
            step: async (_input, _key, producer) => producer(),
        };
        type ProgressHost = WorkflowExecutionHost & {
            receiver: string;
            receipts: Array<{ runId: string; sequence: number; eventId: string; text: string }>;
        };
        const progress = vi.fn(async function (
            this: ProgressHost,
            carrier: WorkflowInvocation,
            event: WorkflowProgressEvent
        ) {
            expect(this.receiver).toBe("new-host");
            this.receipts.push({
                runId: carrier.runId,
                sequence: event.sequence,
                eventId: event.eventId,
                text: event.text,
            });
            if (mutates) {
                event.sequence = 999;
                event.eventId = "sink-mutated";
                event.text = "sink-mutated";
                carrier.runId = "sink-mutated";
            }
        });
        const newHost: ProgressHost = {
            receiver: "new-host",
            receipts: [],
            agent: vi.fn(async (logical, prompt) => {
                expect(logical.runId).toBe("old-native");
                expect(prompt).toBe("second");
                return 9;
            }),
            step: async (_input, _key, producer) => producer(),
            progress,
        };
        const state = (status: OrchestrationStatus, result?: unknown) =>
            new OrchestrationState(
                "instance",
                "copilot.workflow.async.v3",
                status,
                new Date(0),
                new Date(0),
                JSON.stringify(saved),
                result === undefined ? undefined : JSON.stringify(result)
            );
        const scheduleNewOrchestration = vi.fn(async (_name: unknown, input?: unknown) => {
            saved = structuredClone(input);
            return "instance";
        });
        const getOrchestrationState = vi.fn(async () => {
            calls++;
            if (calls === 1) return undefined;
            if (calls === 2) {
                firstOutcome = await invoke("copilot.workflow.agent.v3", {
                    input: saved,
                    history: [],
                    command: { index: 0, kind: "agent", prompt: "first", options: {} },
                });
                controller.abort();
                return state(OrchestrationStatus.RUNNING);
            }
            if (calls === 3) return state(OrchestrationStatus.RUNNING);
            const history = [
                {
                    command: { index: 0, kind: "agent", prompt: "first", options: {} },
                    outcome: firstOutcome,
                },
            ];
            const turn = await invoke("copilot.workflow.advance.v3", { input: saved, history });
            expect(turn).toMatchObject({
                status: "command",
                command: { index: 1, prompt: "second" },
            });
            const second = await invoke("copilot.workflow.agent.v3", {
                input: saved,
                history,
                command: turn.command,
            });
            for (const sequence of [7, 7, 8]) {
                await invoke("copilot.workflow.progress.v3", {
                    input: saved,
                    event: { sequence, kind: "log", text: `line-${sequence}` },
                });
            }
            return state(OrchestrationStatus.COMPLETED, {
                result: { first: 7, second: second.value },
            });
        });
        const executor = new DtsWorkflowExecutor({
            client: { getOrchestrationState, scheduleNewOrchestration },
            worker,
            workflows: [flow],
            codeVersion: "handoff-test",
        });
        await expect(
            executor.execute(definition, original, oldHost, controller.signal)
        ).rejects.toMatchObject({ name: "AbortError" });
        const continuation = new DtsWorkflowContinuationExecutor(executor, flow, original);
        original.args.input = 99; // Caller mutation must not change the saved trusted mapping.
        await expect(
            continuation.execute(definition, current, newHost, new AbortController().signal)
        ).resolves.toEqual({ result: { first: 7, second: 9 } });
        expect(oldHost.agent).toHaveBeenCalledOnce();
        expect(newHost.agent).toHaveBeenCalledOnce();
        expect(scheduleNewOrchestration).toHaveBeenCalledOnce();
        expect(
            newHost.receipts.map((line) => [line.runId, line.sequence, line.eventId, line.text])
        ).toEqual([
            ["new-native", 0, "scope:new-native:progress:0", "line-7"],
            ["new-native", 0, "scope:new-native:progress:0", "line-7"],
            ["new-native", 1, "scope:new-native:progress:1", "line-8"],
        ]);
    }

    it.each([false, true])(
        "reuses completed work and preserves a method-dependent progress sink (mutates=%s)",
        exerciseProgressHost
    );

    it("rejects absent instances and changed carrier identity without scheduling", async () => {
        const flow = workflow(async () => null);
        const context = setup(flow, {
            agent: async () => null,
            step: async (_input, _key, producer) => producer(),
        });
        const schedule = vi.spyOn(context.binding, "scheduleNewOrchestration");
        const saved = { scopeId: "scope", runId: "old", args: { input: 1 } };
        const continuation = new DtsWorkflowContinuationExecutor(context.executor, flow, saved);
        const definition = getWorkflowDefinition(flow);
        const signal = new AbortController().signal;
        const host: WorkflowExecutionHost = {
            agent: async () => null,
            step: async (_input, _key, producer) => producer(),
        };
        await expect(
            continuation.execute(definition, { ...saved, runId: "new" }, host, signal)
        ).rejects.toThrow("instance is missing");
        for (const current of [
            saved,
            { ...saved, runId: "new", scopeId: "different" },
            { ...saved, runId: "new", args: { input: 2 } },
        ]) {
            await expect(
                continuation.execute(definition, current, host, signal)
            ).rejects.toMatchObject({ code: "workflow_replay_diverged" });
        }
        await expect(
            continuation.execute(
                getWorkflowDefinition(workflow(async () => "changed")),
                { ...saved, runId: "new" },
                host,
                signal
            )
        ).rejects.toMatchObject({ code: "workflow_replay_diverged" });
        expect(schedule).not.toHaveBeenCalled();
    });
});

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
                    "copilot.workflow.async.v3",
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
                    "copilot.workflow.async.v3",
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

    it("rejects a stored v2 instance instead of attaching with v3 replay semantics", async () => {
        const getOrchestrationState = vi
            .fn<TaskHubGrpcClient["getOrchestrationState"]>()
            .mockResolvedValue(
                new OrchestrationState(
                    "instance",
                    "copilot.workflow.async.v2",
                    OrchestrationStatus.COMPLETED,
                    new Date(0),
                    new Date(0),
                    "{}",
                    JSON.stringify({ result: "old result" })
                )
            );
        const scheduleNewOrchestration = vi.fn(async () => "instance");
        const flow = workflow(async () => null);
        const executor = new DtsWorkflowExecutor({
            client: { getOrchestrationState, scheduleNewOrchestration },
            worker: new TestOrchestrationWorker(new InMemoryOrchestrationBackend()),
            workflows: [flow],
            codeVersion: "protocol-pin-test",
            executionHost: {
                agent: async () => null,
                step: async (_input, _key, producer) => producer(),
            },
        });
        await expect(executor.run(flow, invocation)).rejects.toMatchObject({
            code: "workflow_replay_diverged",
        });
        expect(scheduleNewOrchestration).not.toHaveBeenCalled();
    });
});

describe("DTS provider with the SDK in-memory test backend (not restart proof)", () => {
    it("replays a parallel RPC rejection into its branch catch and executes the fallback once", async () => {
        const agent = vi.fn(async (_input, prompt: string) => {
            if (prompt === "left")
                throw new ResponseError(-32603, "left failed", { code: "storage" });
            return "ok";
        });
        const step = vi.fn(async (_input, _key, producer) => producer());
        const flow = workflow(async (ctx) =>
            ctx.parallel([
                async () => {
                    try {
                        return json(await ctx.agent("left"));
                    } catch (error) {
                        if (!(error instanceof ResponseError)) throw error;
                        return await ctx.step("fallback", () => ({ recovered: error.code }));
                    }
                },
                async () => json(await ctx.agent("right")),
            ])
        );
        const context = setup(flow, { agent, step });
        await context.worker.start();
        try {
            await expect(context.executor.run(flow, invocation)).resolves.toEqual({
                result: [{ recovered: -32603 }, "ok"],
            });
            expect(agent.mock.calls.map(([, prompt]) => prompt)).toEqual(["left", "right"]);
            expect(step).toHaveBeenCalledOnce();
            expect(step.mock.calls[0][1]).toBe("fallback");
        } finally {
            await context.worker.stop();
        }
    });

    it("replays captures across successive completed parallel groups without repeating effects", async () => {
        const agent = vi.fn(async (_input, prompt: string) => (prompt === "left" ? 7 : 9));
        const step = vi.fn(async (_input, _key, producer) => producer());
        const flow = workflow(async (ctx) => {
            const values: Record<string, JsonValue> = {};
            await ctx.parallel(
                ["left", "right"].map((side) => async () => {
                    values[side] = json(await ctx.agent(side, { label: side }));
                    return null;
                })
            );
            await ctx.parallel(
                ["left", "right"].map((side) => async () => {
                    const previous = values[side];
                    const repeated = json(await ctx.agent(side, { label: side }));
                    values[side] = { previous, repeated };
                    return null;
                })
            );
            return await ctx.step("report", () => ({ values }));
        });
        const context = setup(flow, { agent, step });
        await context.worker.start();
        try {
            await expect(context.executor.run(flow, invocation)).resolves.toEqual({
                result: {
                    values: {
                        left: { previous: 7, repeated: 7 },
                        right: { previous: 9, repeated: 9 },
                    },
                },
            });
            expect(agent).toHaveBeenCalledTimes(2);
            expect(step).toHaveBeenCalledOnce();
        } finally {
            await context.worker.stop();
        }
    });

    it("propagates an uncaught parallel RPC failure only after admitted branch work completes", async () => {
        const completed: string[] = [];
        const flow = workflow(async (ctx) =>
            ctx.parallel(
                ["left", "right"].map((prompt) => async () => json(await ctx.agent(prompt)))
            )
        );
        const context = setup(flow, {
            agent: async (_input, prompt) => {
                completed.push(prompt);
                if (prompt === "left") throw new ResponseError(-32603, "uncaught branch failure");
                return "ok";
            },
            step: async (_input, _key, producer) => producer(),
        });
        await context.worker.start();
        try {
            await expect(context.executor.run(flow, invocation)).rejects.toThrow(
                "uncaught branch failure"
            );
            expect(completed).toEqual(["left", "right"]);
            const state = await context.client.getOrchestrationState(
                context.executor.getInstanceId(invocation),
                true
            );
            expect(state?.runtimeStatus).toBe(OrchestrationStatus.FAILED);
        } finally {
            await context.worker.stop();
        }
    });

    it("rebuilds parallel branch captures before reconstructing the report producer", async () => {
        const agent = vi.fn(async (_input, prompt: string) => (prompt === "left" ? 7 : 9));
        const step = vi.fn(async (_input, _key, producer) => producer());
        const flow = workflow(async (ctx) => {
            const values: Record<string, JsonValue> = {};
            await ctx.parallel(
                ["left", "right"].map((side) => async () => {
                    values[side] = json(await ctx.agent(side, { label: side }));
                    return null;
                })
            );
            return await ctx.step("report", () => ({ values }));
        });
        const context = setup(flow, { agent, step });
        await context.worker.start();
        try {
            await expect(context.executor.run(flow, invocation)).resolves.toEqual({
                result: { values: { left: 7, right: 9 } },
            });
            expect(agent.mock.calls.map(([, prompt]) => prompt)).toEqual(["left", "right"]);
            expect(step).toHaveBeenCalledOnce();
            expect(step.mock.calls[0][1]).toBe("report");
        } finally {
            await context.worker.stop();
        }
    });

    it("delivers phase and log metadata before propagating an unhandled root failure", async () => {
        const progress = vi.fn(async (_input, _event) => {});
        const flow = workflow(async (ctx) => {
            ctx.phase("Review");
            await ctx.agent("review");
            ctx.phase("Verify");
            ctx.log("Validation failed");
            throw new Error("validation failed");
        });
        const context = setup(flow, {
            agent: async () => "reviewed",
            step: async (_input, _key, producer) => producer(),
            progress,
        });
        await context.worker.start();
        try {
            await expect(context.executor.run(flow, invocation)).rejects.toThrow(
                "validation failed"
            );
            expect(
                progress.mock.calls.map(([, event]) => [event.sequence, event.kind, event.text])
            ).toEqual([
                [0, "phase", "Review"],
                [1, "phase", "Verify"],
                [2, "log", "Validation failed"],
            ]);
            const state = await context.client.getOrchestrationState(
                context.executor.getInstanceId(invocation),
                true
            );
            expect(state?.runtimeStatus).toBe(OrchestrationStatus.FAILED);
            expect(state?.serializedOutput).toBeUndefined();
        } finally {
            await context.worker.stop();
        }
    });

    it("fans out branch effects, shares identical agents/step keys and flushes final progress", async () => {
        const events: string[] = [];
        const agent = vi.fn(async (_input, prompt: string) => {
            events.push(prompt);
            return prompt;
        });
        const step = vi.fn(async (_input, _key, producer) => {
            events.push("report");
            return producer();
        });
        const progress = vi.fn(async (_input, event) => {
            events.push(event.text);
        });
        const flow = workflow(async (ctx) => {
            ctx.phase("Review");
            const values = await ctx.parallel(
                ["left", "right", "left"].map((side) => async () => {
                    const value = json(await ctx.agent(side, { label: side }));
                    return await ctx.step(`branch-${side}`, () => ({ value }));
                })
            );
            ctx.phase("Verify");
            const report = await ctx.step("report", () => ({ values }));
            ctx.log(`Completed workflow run ${ctx.runId}`);
            return report;
        });
        const context = setup(flow, { agent, step, progress });
        await context.worker.start();
        try {
            expect(await context.executor.run(flow, invocation)).toEqual({
                result: { values: [{ value: "left" }, { value: "right" }, { value: "left" }] },
            });
            expect(agent).toHaveBeenCalledTimes(2);
            expect(step).toHaveBeenCalledTimes(3);
            expect(
                progress.mock.calls.map(([, event]) => [event.sequence, event.kind, event.text])
            ).toEqual([
                [0, "phase", "Review"],
                [1, "phase", "Verify"],
                [2, "log", "Completed workflow run run"],
            ]);
            expect(progress.mock.calls.map(([, event]) => event.eventId)).toEqual(
                [0, 1, 2].map(
                    (sequence) =>
                        `${context.executor.getInstanceId(invocation)}:progress:${sequence}`
                )
            );
            expect(events[0]).toBe("Review");
            expect(events[events.length - 1]).toBe("Completed workflow run run");
            expect(events.indexOf("Verify")).toBeGreaterThan(events.indexOf("right"));
            expect(await context.executor.run(flow, invocation)).toEqual({
                result: { values: [{ value: "left" }, { value: "right" }, { value: "left" }] },
            });
            expect(progress).toHaveBeenCalledTimes(3);
        } finally {
            await context.worker.stop();
        }
    });

    it("supports empty parallel and preserves false, zero, empty text, and ordinary-null failures", async () => {
        const flow = workflow(async (ctx) => ({
            empty: await ctx.parallel([]),
            values: await ctx.parallel<JsonValue>([
                () => false,
                () => 0,
                () => "",
                () => {
                    throw new Error("ordinary");
                },
            ]),
        }));
        const host: WorkflowExecutionHost = {
            agent: vi.fn(async () => null),
            step: vi.fn(async (_input, _key, producer) => producer()),
        };
        const context = setup(flow, host);
        await context.worker.start();
        try {
            expect(await context.executor.run(flow, invocation)).toEqual({
                result: { empty: [], values: [false, 0, "", null] },
            });
            expect(host.agent).not.toHaveBeenCalled();
            expect(host.step).not.toHaveBeenCalled();
        } finally {
            await context.worker.stop();
        }
    });

    it.each(["ResponseError", "AbortError"] as const)(
        "drains admitted branch work and propagates %s without flattening it to null",
        async (kind) => {
            const completed: string[] = [];
            const host: WorkflowExecutionHost = {
                agent: async (_input, prompt) => {
                    completed.push(prompt);
                    if (prompt === "fails") {
                        if (kind === "AbortError")
                            throw new DOMException("cancelled", "AbortError");
                        throw new ResponseError(-32603, "storage unavailable", { code: "storage" });
                    }
                    return "other completed";
                },
                step: async (_input, _key, producer) => producer(),
            };
            const flow = workflow(async (ctx) => {
                try {
                    return await ctx.parallel(
                        ["fails", "other"].map(
                            (prompt) => async () => json(await ctx.agent(prompt))
                        )
                    );
                } catch (error) {
                    if (!(error instanceof Error)) throw error;
                    return {
                        failureName: error instanceof ResponseError ? "ResponseError" : error.name,
                    };
                }
            });
            const context = setup(flow, host);
            await context.worker.start();
            try {
                expect(await context.executor.run(flow, invocation)).toEqual({
                    result: { failureName: kind },
                });
                expect(completed).toEqual(["fails", "other"]);
            } finally {
                await context.worker.stop();
            }
        }
    );

    it("fails explicitly for missing or failed progress sinks", async () => {
        const flow = workflow(async (ctx) => {
            ctx.log("visible");
            return "must not silently complete";
        });
        for (const progress of [
            undefined,
            async () => {
                throw new Error("sink failed");
            },
        ]) {
            const context = setup(flow, {
                agent: async () => null,
                step: async (_input, _key, producer) => producer(),
                ...(progress ? { progress } : {}),
            });
            await context.worker.start();
            try {
                await expect(context.executor.run(flow, invocation)).rejects.toThrow(
                    progress ? "sink failed" : "require an executionHost.progress"
                );
            } finally {
                await context.worker.stop();
            }
        }
    });

    it("cannot swallow a parallel producer's profile violation in the outer workflow catch", async () => {
        const completed: string[] = [];
        const flow = workflow(async (ctx) => {
            try {
                return await ctx.parallel([
                    async () =>
                        ctx.step("invalid", async () => {
                            try {
                                await ctx.agent("nested effect");
                            } catch {
                                return "swallowed";
                            }
                            return null;
                        }),
                    async () => json(await ctx.agent("other")),
                ]);
            } catch {
                return "must not complete";
            }
        });
        const context = setup(flow, {
            agent: async (_input, prompt) => {
                completed.push(prompt);
                return prompt;
            },
            step: async (_input, _key, producer) => producer(),
        });
        await context.worker.start();
        try {
            await expect(context.executor.run(flow, invocation)).rejects.toThrow(
                "SDK workflow effects are not allowed"
            );
            expect(completed).toEqual(["other"]);
        } finally {
            await context.worker.stop();
        }
    });

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
