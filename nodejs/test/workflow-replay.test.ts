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

    it.each([
        "parallel",
        "pipeline",
        "pause",
        "phase",
        "log",
        "workflow",
        "session",
        "signal",
    ] as const)(
        "fails closed for unsupported %s even when the body catches it",
        async (operation) => {
            const workflow = definition(async (ctx) => {
                try {
                    switch (operation) {
                        case "parallel":
                            await ctx.parallel([]);
                            break;
                        case "pipeline":
                            await ctx.pipeline([]);
                            break;
                        case "pause":
                            await ctx.pause("key");
                            break;
                        case "phase":
                            ctx.phase("title");
                            break;
                        case "log":
                            ctx.log("text");
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
