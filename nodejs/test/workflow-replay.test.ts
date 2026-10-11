/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it, vi } from "vitest";
import { ResponseError } from "vscode-jsonrpc/node.js";
import {
    defineWorkflow,
    getWorkflowDefinition,
    type JsonValue,
    type WorkflowDefinition,
} from "../src/workflow.js";
import { assertWorkflowStepResult } from "../src/workflow-json.js";
import {
    canonicalWorkflowJson,
    recordWorkflowFailure,
    replayWorkflow,
    type WorkflowHistoryEntry,
    type WorkflowParallelHistory,
} from "../src/workflow-replay.js";

const invocation = { scopeId: "scope", runId: "run", args: { input: 7 } };
const meta = { name: "replay", description: "Replay tests", phases: [] };

function json(value: unknown): JsonValue {
    assertWorkflowStepResult(value, "test");
    return value;
}

function definition(run: WorkflowDefinition["run"]): WorkflowDefinition {
    return getWorkflowDefinition(defineWorkflow({ meta, run }));
}

describe("sequential async workflow replay (no scheduler)", () => {
    it("exposes suspension without entering user catch or finally", async () => {
        const caught = vi.fn();
        const final = vi.fn();
        const workflow = definition(async (ctx) => {
            try {
                return json(await ctx.agent("inspect", { label: "one" }));
            } catch {
                caught();
                return "incorrect";
            } finally {
                final();
            }
        });
        const first = await replayWorkflow(workflow, invocation, []);
        expect(first.turn).toEqual({
            status: "command",
            command: { index: 0, kind: "agent", prompt: "inspect", options: { label: "one" } },
        });
        expect(caught).not.toHaveBeenCalled();
        expect(final).not.toHaveBeenCalled();
        if (first.turn.status !== "command") throw new Error("Missing frontier");
        const history: WorkflowHistoryEntry[] = [
            {
                command: first.turn.command,
                outcome: { status: "fulfilled", value: { answer: 42 } },
            },
        ];
        expect((await replayWorkflow(workflow, invocation, history)).turn).toEqual({
            status: "completed",
            result: { answer: 42 },
        });
        expect(caught).not.toHaveBeenCalled();
        expect(final).toHaveBeenCalledOnce();
    });

    it("replays recorded RPC rejections into the author's catch, not as null", async () => {
        const workflow = definition(async (ctx) => {
            try {
                return json(await ctx.agent("inspect"));
            } catch (error) {
                if (!(error instanceof ResponseError)) throw error;
                return { code: error.code, data: error.data };
            }
        });
        const first = await replayWorkflow(workflow, invocation, []);
        if (first.turn.status !== "command") throw new Error("Missing frontier");
        const recorded = recordWorkflowFailure(
            new ResponseError(-32603, "storage unavailable", {
                code: "workflow_storage_unavailable",
            })
        );
        expect(
            (
                await replayWorkflow(workflow, invocation, [
                    {
                        command: first.turn.command,
                        outcome: { status: "rejected", error: recorded },
                    },
                ])
            ).turn
        ).toEqual({
            status: "completed",
            result: { code: -32603, data: { code: "workflow_storage_unavailable" } },
        });
    });

    it("clones fixed outcomes before the body can mutate them", async () => {
        const workflow = definition(async (ctx) => {
            const value = await ctx.step("fixed", () => ({ count: 1 }));
            if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
            value.count = 99;
            return value;
        });
        const first = await replayWorkflow(workflow, invocation, []);
        if (first.turn.status !== "command") throw new Error("Missing frontier");
        const history: WorkflowHistoryEntry[] = [
            { command: first.turn.command, outcome: { status: "fulfilled", value: { count: 1 } } },
        ];
        expect((await replayWorkflow(workflow, invocation, history)).turn).toMatchObject({
            result: { count: 99 },
        });
        expect(history[0].outcome).toEqual({ status: "fulfilled", value: { count: 1 } });
    });

    it("recreates a step closure with values from its replayed prefix", async () => {
        const produced = vi.fn();
        const workflow = definition(async (ctx) => {
            const previous = json(await ctx.agent("inspect"));
            return await ctx.step("report", () => {
                produced();
                return { previous };
            });
        });
        const first = await replayWorkflow(workflow, invocation, []);
        if (first.turn.status !== "command") throw new Error("Missing frontier");
        const next = await replayWorkflow(workflow, invocation, [
            { command: first.turn.command, outcome: { status: "fulfilled", value: 42 } },
        ]);
        expect(next.turn).toMatchObject({ status: "command", command: { index: 1, kind: "step" } });
        expect(produced).not.toHaveBeenCalled();
        await expect(next.producer?.()).resolves.toEqual({ previous: 42 });
        expect(produced).toHaveBeenCalledOnce();
    });

    it("does not let a catch block absorb command divergence", async () => {
        const workflow = definition(async (ctx) => {
            try {
                return json(await ctx.agent("changed"));
            } catch {
                return "swallowed";
            }
        });
        await expect(
            replayWorkflow(workflow, invocation, [
                {
                    command: { index: 0, kind: "agent", prompt: "original", options: {} },
                    outcome: { status: "fulfilled", value: "old" },
                },
            ])
        ).rejects.toMatchObject({ code: "workflow_replay_diverged" });
    });

    it("rejects a body that omits part of the recorded prefix", async () => {
        await expect(
            replayWorkflow(
                definition(async () => null),
                invocation,
                [
                    {
                        command: { index: 0, kind: "agent", prompt: "original", options: {} },
                        outcome: { status: "fulfilled", value: null },
                    },
                ]
            )
        ).rejects.toMatchObject({ code: "workflow_replay_diverged" });
    });

    it.each(["pipeline", "pause", "workflow", "session", "signal"] as const)(
        "fails closed for unsupported %s even when the body catches it",
        async (operation) => {
            const workflow = definition(async (ctx) => {
                try {
                    switch (operation) {
                        case "pipeline":
                            await ctx.pipeline([]);
                            break;
                        case "pause":
                            await ctx.pause("key");
                            break;
                        case "workflow":
                            await ctx.workflow("nested");
                            break;
                        case "session":
                            void ctx.session;
                            break;
                        case "signal":
                            void ctx.signal;
                            break;
                    }
                } catch {
                    return "swallowed";
                }
                return null;
            });
            await expect(replayWorkflow(workflow, invocation, [])).rejects.toMatchObject({
                code: "workflow_effect_unsupported",
            });
        }
    );

    it("discovers parallel effects without entering catch/finally and replays actual completion order", async () => {
        const caught = vi.fn();
        const final = vi.fn();
        const workflow = definition(async (ctx) => {
            const seen: string[] = [];
            try {
                return await ctx.parallel(
                    ["left", "right"].map((side) => async () => {
                        const value = json(await ctx.agent(side, { label: side }));
                        seen.push(side);
                        return { value, seen: [...seen] };
                    })
                );
            } catch {
                caught();
                return null;
            } finally {
                final();
            }
        });
        const initial = await replayWorkflow(workflow, invocation, []);
        expect(initial.turn.status).toBe("parallel");
        if (initial.turn.status !== "parallel") throw new Error("Missing parallel turn");
        const commands = initial.turn.branches.map((branch) => {
            if (branch.status !== "command") throw new Error("Missing branch command");
            return branch.command;
        });
        expect(commands.map((command) => command.kind)).toEqual(["agent", "agent"]);
        expect(caught).not.toHaveBeenCalled();
        expect(final).not.toHaveBeenCalled();
        const parallel: WorkflowParallelHistory = {
            branches: commands.map((command, branch) => [
                { command, outcome: { status: "fulfilled", value: branch } },
            ]),
            completionOrder: [
                { branch: 1, index: 0 },
                { branch: 0, index: 0 },
            ],
        };
        const next = await replayWorkflow(workflow, invocation, [], undefined, parallel);
        if (next.turn.status !== "parallel") throw new Error("Missing parallel replay");
        expect(next.turn.branches).toEqual([
            { status: "completed", result: { value: 0, seen: ["right", "left"] } },
            { status: "completed", result: { value: 1, seen: ["right"] } },
        ]);
        expect(final).not.toHaveBeenCalled();
        const values = next.turn.branches.map((branch) => {
            if (branch.status !== "completed") throw new Error("Incomplete branch");
            return branch.result;
        });
        expect(
            (
                await replayWorkflow(workflow, invocation, [
                    {
                        command: initial.turn.command,
                        outcome: { status: "fulfilled", value: values },
                        parallel,
                    },
                ])
            ).turn
        ).toEqual({ status: "completed", result: values });
        expect(final).toHaveBeenCalledOnce();
    });

    it("collects root progress including the final log without changing phase/log's void API", async () => {
        const workflow = definition(async (ctx) => {
            expect(ctx.phase("Review")).toBeUndefined();
            await ctx.agent("review");
            ctx.phase("Verify");
            ctx.log("Finished");
            return null;
        });
        const first = await replayWorkflow(workflow, invocation, []);
        if (first.turn.status !== "command") throw new Error("Missing command");
        expect(first.turn.progress).toEqual([{ sequence: 0, kind: "phase", text: "Review" }]);
        const next = await replayWorkflow(workflow, invocation, [
            { command: first.turn.command, outcome: { status: "fulfilled", value: "reviewed" } },
        ]);
        expect(next.turn).toEqual({
            status: "completed",
            result: null,
            progress: [
                { sequence: 0, kind: "phase", text: "Review" },
                { sequence: 1, kind: "phase", text: "Verify" },
                { sequence: 2, kind: "log", text: "Finished" },
            ],
        });
    });

    it.each(["nested parallel", "branch progress", "producer progress"] as const)(
        "rejects unsupported %s without letting user catch bypass the profile",
        async (operation) => {
            const workflow = definition(async (ctx) => {
                if (operation === "producer progress") {
                    return await ctx.step("progress", () => {
                        try {
                            ctx.log("not allowed");
                        } catch {
                            return null;
                        }
                        return null;
                    });
                }
                return await ctx.parallel([
                    async () => {
                        try {
                            if (operation === "nested parallel") return await ctx.parallel([]);
                            ctx.phase("not allowed");
                        } catch {
                            return null;
                        }
                        return null;
                    },
                ]);
            });
            if (operation === "producer progress") {
                const capture = await replayWorkflow(workflow, invocation, []);
                await expect(capture.producer?.()).rejects.toMatchObject({
                    code: "workflow_effect_unsupported",
                });
            } else {
                await expect(replayWorkflow(workflow, invocation, [])).rejects.toMatchObject({
                    code: "workflow_effect_unsupported",
                });
            }
        }
    );

    it("distinguishes ordinary branch throws from fatal runtime errors", async () => {
        const workflow = definition(async (ctx) =>
            ctx.parallel([
                () => {
                    throw new Error("ordinary");
                },
                () => {
                    throw new ResponseError(-32603, "fatal", { code: "limit" });
                },
            ])
        );
        const result = await replayWorkflow(workflow, invocation, []);
        if (result.turn.status !== "parallel") throw new Error("Missing parallel turn");
        expect(result.turn.branches[0]).toEqual({ status: "completed", result: null });
        expect(result.turn.branches[1]).toMatchObject({
            status: "failed",
            error: { kind: "error", rpcCode: -32603, data: { code: "limit" } },
        });
    });

    it("rejects missing or duplicated completion history", async () => {
        const workflow = definition(async (ctx) =>
            ctx.parallel([async () => json(await ctx.agent("one"))])
        );
        const initial = await replayWorkflow(workflow, invocation, []);
        if (initial.turn.status !== "parallel" || initial.turn.branches[0].status !== "command") {
            throw new Error("Missing branch frontier");
        }
        const branch = [
            {
                command: initial.turn.branches[0].command,
                outcome: { status: "fulfilled" as const, value: 1 },
            },
        ];
        for (const completionOrder of [
            [],
            [
                { branch: 0, index: 0 },
                { branch: 0, index: 0 },
            ],
        ]) {
            await expect(
                replayWorkflow(workflow, invocation, [], undefined, {
                    branches: [branch],
                    completionOrder,
                })
            ).rejects.toMatchObject({ code: "workflow_replay_diverged" });
        }
    });

    it("rejects volatile and nested producer effects", async () => {
        await expect(
            replayWorkflow(
                definition(async (ctx) => ctx.step("key", () => 1, { volatile: true })),
                invocation,
                []
            )
        ).rejects.toMatchObject({ code: "workflow_effect_unsupported" });
        const capture = await replayWorkflow(
            definition(async (ctx) =>
                ctx.step("key", async () => {
                    try {
                        await ctx.agent("nested");
                    } catch {
                        return "swallowed";
                    }
                    return null;
                })
            ),
            invocation,
            []
        );
        await expect(capture.producer?.()).rejects.toMatchObject({
            code: "workflow_effect_unsupported",
        });
    });

    it.each(["all", "race", "unawaited"] as const)(
        "rejects raw Promise %s rather than inventing completion order",
        async (operation) => {
            const workflow = definition(async (ctx) => {
                if (operation === "unawaited") {
                    void ctx.agent("one");
                    return null;
                }
                const calls = [ctx.agent("one").then(json), ctx.agent("two").then(json)];
                if (operation === "race") return await Promise.race(calls);
                return await Promise.all(calls);
            });
            await expect(replayWorkflow(workflow, invocation, [])).rejects.toMatchObject({
                code: "workflow_effect_unsupported",
            });
        }
    );

    it("diagnoses uncontrolled promises and async timer effects", async () => {
        await expect(
            replayWorkflow(
                definition(async () => new Promise(() => {})),
                invocation,
                []
            )
        ).rejects.toMatchObject({
            code: "workflow_effect_unsupported",
            message: expect.stringContaining("promise"),
        });
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
            await expect(
                replayWorkflow(
                    definition(
                        async () =>
                            new Promise((resolve) => {
                                timer = setTimeout(() => resolve(null), 1000);
                            })
                    ),
                    invocation,
                    []
                )
            ).rejects.toMatchObject({
                code: "workflow_effect_unsupported",
                message: expect.stringContaining("Timeout"),
            });
        } finally {
            clearTimeout(timer);
        }
    });

    it("allows pure promise continuations and preserves undefined versus JSON null", async () => {
        expect(
            (
                await replayWorkflow(
                    definition(async () => {
                        await Promise.resolve();
                    }),
                    invocation,
                    []
                )
            ).turn
        ).toEqual({ status: "completed" });
        expect(
            (
                await replayWorkflow(
                    definition(async () => null),
                    invocation,
                    []
                )
            ).turn
        ).toEqual({ status: "completed", result: null });
    });

    it.each([Number.NaN, -0, 1n, { nested: undefined }, new Date()])(
        "rejects non-lossless JSON %s",
        async (value) => {
            expect(() => canonicalWorkflowJson(value)).toThrow();
        }
    );
});
