/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const node20 = process.argv[2];
const npmCli = process.env.npm_execpath;
if (!node20 || !npmCli) {
    throw new Error("Run npm run test:workflow-node20 -- <path-to-Node-20.19.0-binary>");
}
const version = execFileSync(node20, ["--version"], { encoding: "utf8" }).trim();
assert.equal(version, "v20.19.0", "This regression checks the minimum supported Node runtime");
const nodeRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const registry = "https://packagefeedproxy.microsoft.io/npm/";
const directory = await mkdtemp(join(tmpdir(), "copilot-sdk-workflow-node20-"));
try {
    const packed = JSON.parse(
        execFileSync(
            process.execPath,
            [npmCli, "pack", "--ignore-scripts", "--json", "--pack-destination", directory],
            { cwd: nodeRoot, encoding: "utf8" }
        )
    );
    const archive = join(directory, packed[0].filename);
    const manifest = JSON.parse(await readFile(join(nodeRoot, "package.json"), "utf8"));
    assert.equal(manifest.dependencies?.["@microsoft/durabletask-js"], undefined);
    assert.equal(manifest.peerDependencies?.["@microsoft/durabletask-js"], "0.4.0");
    assert.equal(manifest.peerDependenciesMeta?.["@microsoft/durabletask-js"]?.optional, true);
    await writeFile(
        join(directory, "package.json"),
        JSON.stringify({
            name: "copilot-sdk-workflow-node20-consumer",
            private: true,
            type: "module",
        })
    );
    execFileSync(
        node20,
        [
            npmCli,
            "install",
            archive,
            "--engine-strict",
            "--omit=dev",
            "--ignore-scripts",
            `--registry=${registry}`,
            "--no-audit",
            "--no-fund",
            "--fetch-retries=0",
            "--fetch-timeout=30000",
        ],
        { cwd: directory, stdio: "inherit" }
    );
    const installed = join(directory, "node_modules", "@github", "copilot-sdk");
    const installedManifest = JSON.parse(await readFile(join(installed, "package.json"), "utf8"));
    assert.equal(installedManifest.peerDependencies?.["@microsoft/durabletask-js"], "0.4.0");
    assert.equal(
        installedManifest.peerDependenciesMeta?.["@microsoft/durabletask-js"]?.optional,
        true
    );
    assert.equal(
        existsSync(join(directory, "node_modules", "@microsoft", "durabletask-js")),
        false
    );
    await copyFile(
        join(nodeRoot, "test", "e2e", "fixtures", "workflow-local-smoke.mjs"),
        join(directory, "smoke.mjs")
    );
    execFileSync(node20, [join(directory, "smoke.mjs")], { cwd: directory, stdio: "inherit" });
    console.log(
        "NODE20_PACK_PROOF",
        JSON.stringify({
            runtime: version,
            engineStrict: true,
            optionalDtsInstalled: false,
            peerVersion: installedManifest.peerDependencies["@microsoft/durabletask-js"],
            localSmoke: "passed",
            registry,
        })
    );
    execFileSync(
        process.execPath,
        [
            npmCli,
            "install",
            "@microsoft/durabletask-js@0.4.0",
            "--engine-strict",
            "--ignore-scripts",
            `--registry=${registry}`,
            "--no-audit",
            "--no-fund",
            "--fetch-retries=0",
            "--fetch-timeout=30000",
        ],
        { cwd: directory, stdio: "inherit" }
    );
    execFileSync(
        process.execPath,
        [
            "--input-type=module",
            "-e",
            "import assert from 'node:assert/strict';" +
                "import { createRequire } from 'node:module';" +
                "import { DtsWorkflowExecutor } from '@github/copilot-sdk/workflow-dts';" +
                "const require = createRequire(import.meta.url);" +
                "assert.equal(typeof DtsWorkflowExecutor, 'function');" +
                "assert.equal(typeof require('@github/copilot-sdk/workflow-dts').DtsWorkflowExecutor, 'function');" +
                "console.log('Packed optional DTS provider ESM/CJS exports passed');",
        ],
        { cwd: directory, stdio: "inherit" }
    );
    console.log(
        "DTS_PROVIDER_PACK_PROOF",
        JSON.stringify({
            runtime: process.version,
            engineStrict: true,
            peerVersion: "0.4.0",
            exports: ["esm", "commonjs"],
            registry,
        })
    );
} finally {
    await rm(directory, { recursive: true, force: true });
}
