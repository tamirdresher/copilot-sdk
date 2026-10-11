import { appendFileSync, existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { setTimeout } from "node:timers/promises";
import { Metadata } from "@grpc/grpc-js";
import { NoOpLogger, TaskHubGrpcClient, TaskHubGrpcWorker } from "@microsoft/durabletask-js";
import { reviewChanged } from "../../../samples/review-changed.mjs";
import { DtsWorkflowExecutor } from "../../../dist/workflow-dts.js";

const [hostAddress, directory, runId, mode] = process.argv.slice(2);
if (!hostAddress || !directory || !runId || !["park", "complete"].includes(mode)) {
    throw new Error("Expected endpoint, observation directory, run ID, and worker mode");
}
const writeRecord = (name, value) => {
    const target = join(directory, name);
    writeFileSync(`${target}.tmp`, JSON.stringify(value));
    renameSync(`${target}.tmp`, target);
};
const record = (stage, operationId) =>
    appendFileSync(
        join(directory, "activities.jsonl"),
        JSON.stringify({ stage, operationId, workerPid: process.pid, mode }) + "\n"
    );
const metadataGenerator = async () => {
    const metadata = new Metadata();
    metadata.set("taskhub", "default");
    return metadata;
};
const logger = new NoOpLogger();
const client = new TaskHubGrpcClient({ hostAddress, logger, metadataGenerator });
const worker = new TaskHubGrpcWorker({
    hostAddress,
    logger,
    metadataGenerator,
    shutdownTimeoutMs: 2000,
});
const invocation = {
    scopeId: "sdk-parallel-restart-test",
    runId,
    args: { files: ["slow.js", "fast.js"] },
};
const observedProgress = new Set(
    existsSync(join(directory, "progress.jsonl"))
        ? readFileSync(join(directory, "progress.jsonl"), "utf8")
              .trim()
              .split("\n")
              .map((line) => JSON.parse(line).eventId)
        : []
);
const executor = new DtsWorkflowExecutor({
    client,
    worker,
    workflows: [reviewChanged],
    codeVersion: "published-parallel-fixture-v2",
    executionHost: {
        agent: async (activity, prompt, options) => {
            const stage =
                prompt === "Review slow.js"
                    ? "slow"
                    : prompt === "Review fast.js"
                      ? "fast"
                      : undefined;
            if (!stage || options.label !== prompt)
                throw new Error("Published agent command changed");
            record(stage, activity.operationId);
            writeRecord(`${stage}-${mode}-started.json`, { operationId: activity.operationId });
            if (stage === "slow" && mode === "park") await new Promise(() => {});
            if (stage === "fast") {
                while (!existsSync(join(directory, "release-fast"))) await setTimeout(20);
            }
            const result = { file: `${stage}.js`, review: `${stage} reviewed` };
            writeRecord(`${stage}-${mode}-finished.json`, result);
            return result;
        },
        step: async (activity, key, producer) => {
            record(key, activity.operationId);
            return await producer();
        },
        progress: async (_invocation, event) => {
            if (observedProgress.has(event.eventId)) return;
            appendFileSync(join(directory, "progress.jsonl"), JSON.stringify(event) + "\n");
            observedProgress.add(event.eventId);
        },
    },
});
try {
    await worker.start();
    writeRecord(`ready-${mode}.json`, {
        pid: process.pid,
        instanceId: executor.getInstanceId(invocation),
    });
    const result = await executor.run(reviewChanged, invocation);
    writeRecord("completed.json", { instanceId: executor.getInstanceId(invocation), ...result });
} catch (error) {
    writeRecord("error.json", { message: String(error) });
    process.exitCode = 1;
} finally {
    await worker.stop();
    await client.stop();
}
