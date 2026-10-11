/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import {
    HistoryEventType,
    NoOpLogger,
    OrchestrationStatus,
    TaskHubGrpcClient,
} from "@microsoft/durabletask-js";
import { Metadata } from "@grpc/grpc-js";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { setTimeout } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";

const nodeRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const fixture = fileURLToPath(new URL("../e2e/fixtures/workflow-dts-worker.mjs", import.meta.url));
const parallelFixture = fileURLToPath(
    new URL("../e2e/fixtures/workflow-dts-parallel-worker.mjs", import.meta.url)
);
const image =
    "mcr.microsoft.com/dts/dts-emulator@sha256:361323065a608d605f9d3ae56b854eb11f1c47fcc16845a37f948f03ca9c5fac";

function docker(...args: string[]): string {
    const result = spawnSync("docker", args, { encoding: "utf8", timeout: 30_000 });
    if (result.error || result.status !== 0) {
        throw new Error(`docker ${args[0]} failed: ${result.error ?? result.stderr}`);
    }
    return result.stdout.trim();
}

async function waitFor(
    check: () => Promise<boolean> | boolean,
    description: string
): Promise<void> {
    const deadline = Date.now() + 120_000;
    while (!(await check())) {
        if (Date.now() >= deadline) throw new Error(`Timed out: ${description}`);
        await setTimeout(100);
    }
}

async function killOwned(child: ChildProcess): Promise<void> {
    if (child.exitCode !== null || child.signalCode !== null) return;
    if (!child.pid) throw new Error("Worker process was not started");
    child.kill("SIGKILL");
    await waitFor(() => child.exitCode !== null || child.signalCode !== null, "worker exit");
}

async function proveRestart(
    scenario: "sequential" | "published-parallel" | "parallel-captures"
): Promise<void> {
    const artifactRoot = resolve(process.env.COPILOT_DTS_TEST_ARTIFACT_DIR ?? nodeRoot);
    await mkdir(artifactRoot, { recursive: true });
    const directory = await mkdtemp(join(artifactRoot, ".dts-restart-"));
    const runId = randomUUID();
    const containerName = `copilot-sdk-dts-${runId}`;
    let containerId: string | undefined;
    let client: TaskHubGrpcClient | undefined;
    const children: ChildProcess[] = [];
    let childOutput = "";
    const start = (endpoint: string, mode: string): ChildProcess => {
        const child = spawn(
            process.execPath,
            [
                scenario === "sequential" ? fixture : parallelFixture,
                endpoint,
                directory,
                runId,
                mode,
                scenario,
            ],
            {
                cwd: nodeRoot,
                stdio: ["ignore", "pipe", "pipe"],
            }
        );
        children.push(child);
        child.stdout?.on("data", (data) => {
            childOutput += String(data);
        });
        child.stderr?.on("data", (data) => {
            childOutput += String(data);
        });
        child.on("error", (error) => {
            childOutput += String(error);
        });
        return child;
    };
    const observed = (name: string): boolean => {
        if (existsSync(join(directory, "error.json"))) {
            throw new Error(readFileSync(join(directory, "error.json"), "utf8"));
        }
        return existsSync(join(directory, name));
    };
    try {
        containerId = docker(
            "run",
            "--detach",
            "--rm",
            "--name",
            containerName,
            "--publish",
            "127.0.0.1::8080",
            "--publish",
            "127.0.0.1::8082",
            image
        );
        expect(containerId).toMatch(/^[a-f0-9]{64}$/);
        const port = docker(
            "inspect",
            "--format",
            '{{(index (index .NetworkSettings.Ports "8080/tcp") 0).HostPort}}',
            containerId
        );
        expect(port).toMatch(/^\d+$/);
        const endpoint = `127.0.0.1:${port}`;
        client = new TaskHubGrpcClient({
            hostAddress: endpoint,
            logger: new NoOpLogger(),
            metadataGenerator: async () => {
                const metadata = new Metadata();
                metadata.set("taskhub", "default");
                return metadata;
            },
        });
        const scheduler = client;
        let lastReadinessError: unknown;
        try {
            await waitFor(async () => {
                try {
                    await scheduler.getOrchestrationState(`readiness-${runId}`);
                    return true;
                } catch (error) {
                    lastReadinessError = error;
                    return false;
                }
            }, "emulator gRPC readiness");
        } catch (error) {
            throw new Error(`Emulator readiness failed: ${String(lastReadinessError)}`, {
                cause: error,
            });
        }
        const firstWorker = start(endpoint, "park");
        await waitFor(() => observed("ready-park.json"), "worker A ready");
        if (scenario === "sequential") {
            await waitFor(
                () => observed("second-park.json"),
                "second activity entered in worker A"
            );
        } else {
            await waitFor(
                () => observed("slow-park-started.json") && observed("fast-park-started.json"),
                "both distinct parallel agent activities entered before either finishes"
            );
        }
        const readyA = JSON.parse(readFileSync(join(directory, "ready-park.json"), "utf8"));
        expect(readyA.pid).toBe(firstWorker.pid);
        if (typeof readyA.instanceId !== "string")
            throw new Error("Missing scheduler instance identity");
        const instanceId = readyA.instanceId;
        const before = await scheduler.getOrchestrationHistory(instanceId);
        const agents = before.filter(
            (event) =>
                event.type === HistoryEventType.TaskScheduled &&
                event.name === "copilot.workflow.agent.v3"
        );
        expect(agents).toHaveLength(2);
        if (scenario !== "sequential") {
            expect(
                before.filter(
                    (event) =>
                        event.type === HistoryEventType.TaskCompleted &&
                        agents.some((agent) => event.taskScheduledId === agent.eventId)
                )
            ).toHaveLength(0);
            await writeFile(join(directory, "release-fast"), "release");
            await waitFor(async () => {
                const events = await scheduler.getOrchestrationHistory(instanceId);
                return events.some(
                    (event) =>
                        event.type === HistoryEventType.TaskCompleted &&
                        event.taskScheduledId === agents[1].eventId
                );
            }, "fast branch completion recorded while slow branch is still pending");
        } else {
            expect(
                before.some(
                    (event) =>
                        event.type === HistoryEventType.TaskCompleted &&
                        event.taskScheduledId === agents[0].eventId
                )
            ).toBe(true);
        }
        const firstCompletion = before.findIndex(
            (event) =>
                event.type === HistoryEventType.TaskCompleted &&
                event.taskScheduledId === agents[0].eventId
        );
        const secondSchedule = before.findIndex(
            (event) =>
                event.eventId === agents[1].eventId && event.type === HistoryEventType.TaskScheduled
        );
        if (scenario === "sequential") expect(firstCompletion).toBeLessThan(secondSchedule);
        await killOwned(firstWorker);
        const secondWorker = start(endpoint, "complete");
        await waitFor(() => observed("ready-complete.json"), "worker B ready");
        await waitFor(() => observed("completed.json"), "cold worker B completed");
        await waitFor(() => secondWorker.exitCode !== null, "worker B graceful exit");
        expect(secondWorker.exitCode, childOutput).toBe(0);
        const readyB = JSON.parse(readFileSync(join(directory, "ready-complete.json"), "utf8"));
        expect(readyB.pid).not.toBe(readyA.pid);
        expect(readyB.instanceId).toBe(instanceId);
        const completed = JSON.parse(readFileSync(join(directory, "completed.json"), "utf8"));
        const expected =
            scenario === "sequential"
                ? {
                      first: { value: 7 },
                      second: { verified: true, input: { first: { value: 7 } } },
                  }
                : scenario === "parallel-captures"
                  ? {
                        values: {
                            "slow.js": { file: "slow.js", review: "slow reviewed" },
                            "fast.js": { file: "fast.js", review: "fast reviewed" },
                        },
                    }
                  : {
                        reviews: [
                            { file: "slow.js", review: "slow reviewed" },
                            { file: "fast.js", review: "fast reviewed" },
                        ],
                    };
        expect(completed).toEqual({
            instanceId,
            result: expected,
        });
        const records: Array<{ stage: string; operationId: string; workerPid: number }> =
            readFileSync(join(directory, "activities.jsonl"), "utf8")
                .trim()
                .split("\n")
                .map((line) => JSON.parse(line));
        const singleStage = scenario === "sequential" ? "first" : "fast";
        const retriedStage = scenario === "sequential" ? "second" : "slow";
        expect(records.filter((record) => record.stage === singleStage)).toHaveLength(1);
        const second = records.filter((record) => record.stage === retriedStage);
        expect(second).toHaveLength(2);
        expect(second[0].operationId).toBe(second[1].operationId);
        expect(second[0].workerPid).toBe(readyA.pid);
        expect(second[1].workerPid).toBe(readyB.pid);
        expect(records.filter((record) => record.stage === "report")).toHaveLength(1);
        const parked = JSON.parse(
            readFileSync(
                join(
                    directory,
                    scenario === "sequential" ? "second-park.json" : "slow-park-started.json"
                ),
                "utf8"
            )
        );
        const recovered = JSON.parse(
            readFileSync(
                join(
                    directory,
                    scenario === "sequential"
                        ? "second-complete.json"
                        : "slow-complete-started.json"
                ),
                "utf8"
            )
        );
        expect(recovered).toEqual(parked);
        const finalHistory = await scheduler.getOrchestrationHistory(instanceId);
        let progress: unknown;
        if (scenario !== "sequential") {
            const fastCompletion = finalHistory.findIndex(
                (event) =>
                    event.type === HistoryEventType.TaskCompleted &&
                    event.taskScheduledId === agents[1].eventId
            );
            const slowCompletion = finalHistory.findIndex(
                (event) =>
                    event.type === HistoryEventType.TaskCompleted &&
                    event.taskScheduledId === agents[0].eventId
            );
            expect(fastCompletion).toBeLessThan(slowCompletion);
            expect(secondSchedule).toBeLessThan(fastCompletion);
            const lines = readFileSync(join(directory, "progress.jsonl"), "utf8")
                .trim()
                .split("\n")
                .map((line) => JSON.parse(line));
            expect(lines.map(({ sequence, kind, text }) => ({ sequence, kind, text }))).toEqual([
                { sequence: 0, kind: "phase", text: "Review" },
                { sequence: 1, kind: "phase", text: "Verify" },
                { sequence: 2, kind: "log", text: `Completed workflow run ${runId}` },
            ]);
            expect(new Set(lines.map((line) => line.eventId)).size).toBe(3);
            progress = lines;
        }
        const state = await scheduler.getOrchestrationState(instanceId, true);
        expect(state?.runtimeStatus).toBe(OrchestrationStatus.COMPLETED);
        console.log(
            "DTS_RESTART_PROOF",
            JSON.stringify({
                image,
                scenario,
                containerId,
                instanceId,
                workerA: readyA.pid,
                workerB: readyB.pid,
                recordedActivityStage: singleStage,
                firstActivityExecutions: records.filter((record) => record.stage === singleStage)
                    .length,
                secondActivityExecutions: second.length,
                reportActivityExecutions: records.filter((record) => record.stage === "report")
                    .length,
                operationIds: records.map(({ stage, operationId }) => ({ stage, operationId })),
                progress,
                output: completed.result,
                schedulerKeptAlive: true,
                host: "deterministic fixture (no live model)",
            })
        );
    } catch (error) {
        throw new Error(`${String(error)}\n${childOutput}`, { cause: error });
    } finally {
        for (const child of children) await killOwned(child);
        await client?.stop();
        if (containerId) docker("stop", "--time", "5", containerId);
        if (dirname(directory) !== artifactRoot)
            throw new Error("Unexpected test cleanup directory");
        await rm(directory, { recursive: true, force: true });
    }
}

it.each(["sequential", "published-parallel", "parallel-captures"] as const)(
    "recovers the same %s scheduler instance in an independent cold worker",
    proveRestart
);
