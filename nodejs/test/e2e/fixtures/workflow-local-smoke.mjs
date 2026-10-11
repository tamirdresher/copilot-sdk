/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { CopilotSession } from "@github/copilot-sdk";
import { defineWorkflow } from "@github/copilot-sdk/extension";

assert.equal(process.versions.node, "20.19.0");
const require = createRequire(import.meta.url);
assert.equal(typeof require("@github/copilot-sdk").CopilotSession, "function");
assert.equal(typeof require("@github/copilot-sdk/extension").defineWorkflow, "function");
assert.throws(() => require.resolve("@microsoft/durabletask-js"), { code: "MODULE_NOT_FOUND" });

const journal = new Map();
const calls = [];
const session = new CopilotSession("local-node20", {
    sendRequest: async (method, params) => {
        calls.push(method);
        if (method === "session.workflow.agent") return { result: JSON.parse(params.prompt) };
        if (method === "session.workflow.journal.get")
            return journal.has(params.key)
                ? { hit: true, resultJson: journal.get(params.key) }
                : { hit: false };
        if (method === "session.workflow.journal.put") {
            journal.set(params.key, params.resultJson);
            return {};
        }
        throw new Error(`Unexpected Local RPC: ${method}`);
    },
});
let preparations = 0;
const flow = defineWorkflow({
    meta: { name: "local-smoke", description: "Default Local workflow", phases: [] },
    run: async (ctx) => {
        const prepared = await ctx.step("prepare", () => {
            preparations++;
            return { value: 7 };
        });
        return await ctx.agent(JSON.stringify(prepared), { label: "local" });
    },
});
session.registerWorkflows([flow]);
for (const executionToken of ["attempt-1", "attempt-2"]) {
    assert.deepEqual(
        await session.clientSessionApis.workflow.execute({
            sessionId: session.sessionId,
            runId: "local-run",
            name: "local-smoke",
            executionToken,
            args: {},
        }),
        { result: { value: 7 } }
    );
}
assert.equal(preparations, 1);
assert.equal(calls.filter((method) => method === "session.workflow.agent").length, 2);
console.log("Local Node20 ESM/CJS exports and journaled workflow smoke passed without DTS");
