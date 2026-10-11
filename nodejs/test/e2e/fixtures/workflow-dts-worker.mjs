import { appendFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { NoOpLogger, TaskHubGrpcClient, TaskHubGrpcWorker } from "@microsoft/durabletask-js";
import { Metadata } from "@grpc/grpc-js";
import { defineWorkflow } from "../../../dist/extension.js";
import { DtsWorkflowExecutor } from "../../../dist/workflow-dts.js";

const [hostAddress, directory, runId, mode] = process.argv.slice(2);
if (!hostAddress || !directory || !runId || !["park", "complete"].includes(mode)) {
    throw new Error("Expected endpoint, observation directory, run ID, and worker mode");
}

const workflow = defineWorkflow({
    meta: { name: "restart-proof", description: "Cold-worker restart fixture", phases: [] },
    run: async (ctx) => {
        const first = await ctx.agent("first", { label: "first" });
        const second = await ctx.agent(JSON.stringify({ first }), { label: "second" });
        return await ctx.step("report", () => ({ first, second }));
    },
});

const invocation = { scopeId: "sdk-restart-test", runId, args: { expected: 7 } };
const logger = new NoOpLogger();
const metadataGenerator = async () => {
    const metadata = new Metadata();
    metadata.set("taskhub", "default");
    return metadata;
};
const client = new TaskHubGrpcClient({ hostAddress, logger, metadataGenerator });
const worker = new TaskHubGrpcWorker({
    hostAddress,
    logger,
    metadataGenerator,
    shutdownTimeoutMs: 2000,
});
const writeRecord = (name, value) => {
    const target = join(directory, name);
    writeFileSync(`${target}.tmp`, JSON.stringify(value));
    renameSync(`${target}.tmp`, target);
};
const record = (stage, operationId) => {
    appendFileSync(
        join(directory, "activities.jsonl"),
        JSON.stringify({ stage, operationId, workerPid: process.pid, mode }) + "\n"
    );
};
const executor = new DtsWorkflowExecutor({
    client,
    worker,
    workflows: [workflow],
    codeVersion: "sdk-restart-fixture-v1",
    executionHost: {
        agent: async (activity, prompt, options) => {
            record(options.label, activity.operationId);
            if (options.label === "first") return { value: 7 };
            if (options.label !== "second") throw new Error("Unexpected agent");
            const input = JSON.parse(prompt);
            writeRecord(`second-${mode}.json`, { input, operationId: activity.operationId });
            if (mode === "park") await new Promise(() => {});
            return { verified: true, input };
        },
        step: async (activity, key, producer) => {
            record(key, activity.operationId);
            return await producer();
        },
    },
});

try {
    await worker.start();
    writeRecord(`ready-${mode}.json`, {
        pid: process.pid,
        instanceId: executor.getInstanceId(invocation),
    });
    const result = await executor.run(workflow, invocation);
    writeRecord("completed.json", { instanceId: executor.getInstanceId(invocation), ...result });
} catch (error) {
    writeRecord("error.json", { message: String(error) });
    process.exitCode = 1;
} finally {
    await worker.stop();
    await client.stop();
}
