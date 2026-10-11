# Dynamic Workflows

Dynamic Workflows are extension-authored, session-scoped workflows that coordinate subagents and durable steps. The API is experimental.

## Define and register a workflow

Use `defineWorkflow` and pass the returned handle to `joinSession`:

```js
import { defineWorkflow, joinSession } from "@github/copilot-sdk/extension";

const reviewChanged = defineWorkflow({
    meta: {
        name: "review-changed",
        description:
            "Review changed files and verify the findings. " +
            "args: { files: string[] } — the paths to review.",
        phases: [{ title: "Review" }, { title: "Verify" }],
        argsSchema: {
            type: "object",
            required: ["files"],
            properties: {
                files: { type: "array", items: { type: "string" } },
            },
        },
    },
    run: async (ctx) => {
        ctx.phase("Review");
        const reviews = await ctx.parallel(
            ctx.args.files.map(
                (file) => () => ctx.agent(`Review ${file}`, { label: `Review ${file}` })
            )
        );

        ctx.phase("Verify");
        const report = await ctx.step("report", () => ({ reviews }));
        ctx.log(`Completed workflow run ${ctx.runId}`);
        return report;
    },
});

const session = await joinSession({ workflows: [reviewChanged] });
```

Workflow metadata contains a stable `name`, a human-readable `description`, declared `phases`, an optional `argsSchema`, and optional `limits`. Phase entries contain a `title` and optional `detail`.

## Declaring an argument shape

A workflow that reads `ctx.args` should declare `meta.argsSchema`, as the example above does. The schema records the workflow's expected input contract alongside its registration metadata.

Enforcement covers structure — types, required properties, and enum or const values. Finer constraints such as `minLength`, `pattern`, or `additionalProperties` are recorded in the declaration but not enforced. The accepted vocabulary is the `WorkflowJsonSchema` subset also used for subagent structured output: `type`, `required`, `enum`, `const`, recursive `properties`/`items`, and `anyOf`/`oneOf`/`allOf`. A `type` is one of `null`, `boolean`, `integer`, `number`, `string`, `array`, or `object`, or a non-empty array of those such as `["object", "null"]`. A declaration outside that subset is rejected at registration.

`argsSchema` is optional and backward compatible. A workflow that omits it behaves exactly as before, so state the expected shape in its `description`.

An extension calling `session.workflow.run(...)` is not validated against `argsSchema`; those arguments are typed through `defineWorkflow<TArgs>` instead. A workflow that reads `ctx.args` should still validate it rather than assume a shape because the declared subset does not enforce every constraint and JavaScript callers are not statically typed.

`defineWorkflow<TArgs, TResult>` accepts a `run(context)` function returning `Promise<TResult>`, where `TResult` is `JsonValue | void`. Objects, arrays, strings, numbers, booleans, and `null` are valid results. Returning `undefined` completes the workflow with no result. Other non-JSON values are rejected.

## Workflow context

The `run()` context provides:

* `ctx.runId`: Stable ID reused across resumed attempts.
* `ctx.args`: Invocation arguments, forwarded verbatim. When the caller omits `args`, this is `{}` rather than `undefined`.
* `ctx.agent(prompt, options?)`: Runs one workflow-owned subagent. Options are exactly `label`, `schema`, `model`, `agent`, `reasoningEffort`, and `contextTier`. See [Subagent calls](#subagent-calls).
* `ctx.parallel(thunks)`: Runs thunks concurrently and awaits all of them (a barrier). A thunk that throws becomes `null` in the result array, so one failed item does not lose the rest. Cancellation and hard runtime failures (`ResponseError`, `ConnectionError`) are the exception — those propagate and reject the whole call, because they mean the run itself is in trouble rather than one item having failed. Handle them at run level; do not assume every failure arrives as a `null`. Rejects above 4096 items.
* `ctx.pipeline(items, ...stages)`: Flows each item through every stage without a barrier between stages, so one item can be in a later stage while another is still in an earlier one. Each stage is called as `(previous, item, index)`, where `previous` is the prior stage's result and `item` is the original input. A stage that throws drops that item to `null` and skips its remaining stages, with the same exception for cancellation and hard runtime failures. Rejects above 4096 items.
* `ctx.phase(title)`: Starts a named progress phase. This sets a single run-global value, so calling it from inside concurrent `parallel`/`pipeline` stages races. Call it at run-level transitions and distinguish concurrent work by `label` instead.
* `ctx.log(message)`: Appends a progress line. When a workflow bounds its own coverage (top-N, sampling), log what was dropped.
* `ctx.step(key, producer, options?)`: Journals the producer's JSON result under a stable key so a resume replays it without re-running the producer. A journaled (default) producer must return a JSON-serializable value; `undefined` or a non-JSON value is rejected. Pass `{ volatile: true }` to bypass the journal and run the producer every time.

  The key is the *sole* identity: neither the producer body nor its inputs contribute to it. A resume replays the cached value for a matching key even if the producer has since changed, so version the key (`"scan-v2"`) whenever its inputs or meaning change. Journaled producers are best-effort at-least-once and may run again across crashes or concurrent same-key callers, so keep side effects idempotent.
* `ctx.pause(key)`: Pauses at a durable, one-shot checkpoint. The first attempt records the checkpoint, pauses, and throws `AbortError` after cooperative cancellation. When the run resumes, the workflow starts again and the same checkpoint returns so execution can continue. Call it only from the main workflow flow, not inside `ctx.parallel()` or `ctx.pipeline()`.
* `ctx.session`: The session returned by `joinSession`. It refuses calls that start, resume, or pause a workflow run. Call `extensions_manage` with `operation: "guide"` to read more about the session APIs.
* `ctx.signal`: Cooperative cancellation signal for extension work and subprocesses.
* `ctx.workflow(...)`: Always rejects because nested workflows are not supported.

Workflow-owned subagents are intentionally hidden from `read_agent` and `write_agent`. Use the workflow observability APIs instead.

### Subagent calls

`ctx.agent(prompt, options?)` spawns one workflow-scoped subagent and awaits it. Without a schema it resolves to the subagent's final text. With `options.schema` it resolves to the parsed JSON value.

**Identical calls are memoized into one subagent.** Each call is journaled by its canonical prompt and options, including `label`. Two calls with the same prompt and the same options return one shared result — even when issued concurrently. To spawn N *independent* subagents, give each a unique `label` or vary the prompt:

```js
// One subagent, awaited five times — almost certainly not what you want.
await ctx.parallel([1, 2, 3, 4, 5].map(() => () => ctx.agent("Find a bug")));

// Five independent subagents.
await ctx.parallel(
    [1, 2, 3, 4, 5].map((i) => () => ctx.agent("Find a bug", { label: `finder:${i}` }))
);
```

**An ordinary failure resolves to `null` — it does not throw.** A subagent that errors, returns nothing, or (with a schema) produces output that still fails to parse or match after its one retry resolves `null`. Always guard the result before using it, including a bare `await ctx.agent(...)`:

```js
const finding = await ctx.agent(prompt, { label: "inspector" });
if (!finding) return { finding: null };
```

Cancellation and hard runtime failures — a reached limit, a durable-state failure — reject instead, aborting the run. When filtering results, prefer `v => v !== null` over `Boolean`, which also discards a valid `false`, `0`, or `""`.

**`schema` is a structural subset of JSON Schema, not a validator.** Honored: `type`, `required`, `enum`, `const`, recursive `properties`/`items`, and `anyOf`/`oneOf`/`allOf` — where `oneOf` is treated as `anyOf`, meaning at least one branch matches rather than exactly one. Ignored and *not* enforced: `additionalProperties`, `pattern`, `minLength`/`maxLength`, `format`, numeric ranges, and boolean schemas. Do not rely on an ignored keyword to constrain a result. A schema call retries once on a parse or match failure, so it may spawn twice, and both spawns count toward `maxTotalSubagents`.

### Choosing between pipeline and parallel

Prefer `pipeline` for multi-stage work. It has no barrier between stages, so each item advances as soon as its own prior stage finishes.

Reach for a barrier — `parallel` between stages — only when a stage genuinely needs every prior result at once: deduplicating or merging across the full set, an early exit based on the total, or a prompt that compares one result against the others. Needing to map, filter, or flatten is not a reason to use a barrier; do that inside a pipeline stage. Barrier latency is real: if the slowest of N subagents takes three times the fastest, a barrier wastes the rest of the pool's time.

## Resource limits

Limits may be declared in `meta.limits` and overridden per invocation. Every limit is optional and must be positive when present; an omitted limit leaves that dimension unbounded, except that an omitted `maxConcurrentSubagents` falls back to `maxTotalSubagents`, so a declared total cap also bounds concurrency.

Set a ceiling only from real knowledge of what the workflow costs, or because the user named one. A guessed ceiling does not make a run safer: it stops a healthy run partway with `workflow_limit_reached`, after that run has already spent credits. SDK-initiated `run` and `resume` do not request permission, so a caller that wants a ceiling must set it deliberately from a cost it already knows.

```js
// Only when the cost profile is known, or the user asked for this ceiling.
limits: { maxTotalSubagents: 10 },
```

- `maxConcurrentSubagents`: Positive integer concurrent-subagent cap. Additional subagents wait in a queue. Queueing applies backpressure and does not fail the run.
- `maxTotalSubagents`: Positive integer cumulative admission cap. An attempted subagent beyond the cap ends the attempt with failure kind `maxTotalSubagents`.
- `timeoutSeconds`: Positive finite number of seconds, including positive fractions, capped at `2_147_483.647`. It measures accumulated active-execution time across attempts, including the extension body, subprocess waits, queued-agent waits, and sleeps. Time between attempts is excluded. The timeout is soft because already-running work may take time to stop. Its failure kind is `timeoutSeconds`.
- `maxAiCredits`: Positive finite AI-credit budget for the whole run's workflow subagent subtree, including descendants. AI credits are GitHub Copilot's universal usage metric. This is a soft, post-paid ceiling, so completed or parallel turns can settle above it before the run stops. Accounting is fail-closed: an accounting failure stops a budgeted run rather than allowing untracked use. Its failure kind is `maxAiCredits`.

`maxTotalSubagents`, `timeoutSeconds`, and `maxAiCredits` use reject-and-retry semantics. A rejected attempt ends with run status `error` and `failure.type` set to `workflow_limit_reached`. The failed run keeps its ID, arguments, journal, and accounting. Resume the run with a raised limit when additional work is approved. Previously consumed resources still count.

## Run and resume

Run by registered name or handle:

```ts
const run = await session.workflow.run("review-changed", {
    args: { files: ["src/a.ts"] },
    limits: { maxAiCredits: 3 },
    notifyOnComplete: true,
    logPhaseNames: true,
});

if (run.status === "completed") {
    console.log(run.result);
} else {
    console.error(`run ${run.runId} ended as ${run.status}`, run.failure ?? run.error);
}
```

The name overload is:

```ts
session.workflow.run(
    name: string,
    options?: {
        args?: JsonValue;
        limits?: WorkflowLimitOverrides;
        notifyOnComplete?: boolean;
        logPhaseNames?: boolean;
    },
): Promise<WorkflowRunResult>;
```

Resume by run ID without resending the name or arguments:

```ts
const run = await session.workflow.resume(runId, {
    limits: { maxAiCredits: 6 },
    notifyOnComplete: true,
    logPhaseNames: true,
});
```

The signature is:

```ts
session.workflow.resume(
    runId: string,
    options?: {
        limits?: WorkflowLimitOverrides;
        notifyOnComplete?: boolean;
        logPhaseNames?: boolean;
    },
): Promise<WorkflowRunResult>;
```

Set `notifyOnComplete` to `true` for workflows that are likely to be invoked by an agent, so the originating session is notified when the workflow completes. Set it to `false` for workflows intended to be invoked programmatically, where the caller awaits the result directly. Set `logPhaseNames` to emit workflow phase names to the session transcript. Both options apply to new and resumed runs.

Both resolve with the run envelope (`WorkflowRunResult`) for **every** outcome—`completed`, `error`, `halted`, `paused`, and `cancelled` alike. Inspect `status` and read `result` only when the run completed; a limit breach carries a typed `failure`. A `paused` envelope means that the current attempt settled, not that the durable run is permanently finished. Resume the same run ID to start another attempt with its journal and accounting intact. SDK-initiated `run` and `resume` do not request permission, so they have no declined outcome. They still enforce execution eligibility, input validation, and active-run limits. Recognized pre-execution resume failures throw `WorkflowResumeError`, whose `code` is one of `not_found`, `non_resumable`, `workflow_run_not_resumable`, `already_active`, `workflow_already_running`, `workflow_limits_invalid`, `workflow_session_disposed`, `workflow_storage_unavailable`, or `workflow_storage_corrupt`.

Pause a running attempt from outside its workflow body:

```ts
const paused = await session.workflow.pause(runId);
```

Inside a workflow body, use a durable checkpoint instead:

```ts
await ctx.step("prepare", prepareInput);
await ctx.pause("review-ready");
await ctx.agent("Review the prepared input");
```

The first attempt pauses at `"review-ready"` and ends through cooperative cancellation. On resume, the workflow starts from the beginning, reuses the journaled step, returns from the checkpoint, and continues.

## Observe a run

The calling session can inspect its own workflow runs independently of its current execution eligibility. An empty registry enumerates successfully; losing eligibility does not hide already-admitted runs or terminal history. Observation still requires a valid, non-disposed session, and validation, missing-run, and storage errors still propagate.

Workflow storage requires SQLite support from a custom session filesystem. A provider without that capability reports `workflow_storage_unavailable`, not an empty registry or a local-storage fallback.

Compaction waits at most five seconds for its optional active-workflow summary. If storage fails or times out, compaction logs a warning and continues without that summary; aborting compaction cancels the wait. This does not change the results or error handling of explicit workflow observation calls.

Observation does not restore execution eligibility. Starting, resuming, pausing, or cancelling runs, and workflow-owned agent, log, and journal/checkpoint operations still require eligible credentials (token-based billing or trusted HMAC authentication). Ineligible execution requests fail with JSON-RPC `-32601` and `data.code: "dynamic_workflows_unavailable"`.

```ts
const runs = await session.workflow.listRuns();
const runsPage = await session.workflow.listRuns({
    afterSeq,
    beforeSeq,
    limit,
});
const detail = await session.workflow.getRunDetail(runId);
const progressPage = await session.workflow.getRunProgress(runId, {
    phaseId,
    afterSeq,
    beforeSeq,
    limit,
});
```

- `listRuns()` returns only the runs array from the newest default page of this session's durable workflow runs. This overload preserves the original convenience API.
- `listRuns({ afterSeq, beforeSeq, limit })` returns the full page. Its `oldestSeq`, `newestSeq`, `hasMoreNewer`, and `omittedOlder` fields let callers continue paging without raw RPC calls.
- `getRunDetail(runId)` returns phases, prompt-safe agent summaries, and the latest progress page.
- `getRunProgress(runId, options?)` pages progress forward, backward, by phase, or from the latest tail.

`getRun(runId)` reads the latest run envelope. `pause(runId)` pauses a running attempt and returns its `paused` envelope. `cancel(runId)` cancels a run and returns its terminal envelope.

`waitForRun(runId, options?)` resolves with the current attempt's envelope once it settles into `completed`, `error`, `halted`, `paused`, or `cancelled`. It resolves immediately when the current attempt has already settled:

```ts
const settled = await session.workflow.waitForRun(runId);
if (settled.status === "completed") {
    console.log(settled.result);
}
```

It watches `workflow.run_updated` and re-reads the durable envelope on each invalidation, collapsing a burst of events into a single in-flight read. A low-frequency periodic re-read runs alongside the subscription, so a dropped or missing invalidation degrades into a slightly late resolution rather than an unbounded wait. Pass a `signal` to stop waiting:

```ts
const controller = new AbortController();
setTimeout(() => controller.abort(), 30_000);
const settled = await session.workflow.waitForRun(runId, { signal: controller.signal });
```

Aborting rejects the wait and has no effect on the run, which keeps executing—use `pause(runId)` or `cancel(runId)` to stop it. The resolved object is a snapshot of that settled attempt. If its status is `paused`, a later resume updates the durable envelope under the same run ID. Call `getRun(runId)` to read the latest envelope. `isWorkflowRunTerminal(status)` exposes the same current-attempt settlement test for callers driving their own loop.

Listen for the ephemeral `workflow.run_updated` event. Its `{ runId, revision }` payload is an invalidation signal. Re-read the desired API when a newer monotonic revision arrives.

Revisions cover durable lifecycle, accounting, phase, agent, and progress changes. Continuous read-time fields can change without a new revision. These include `observedAt`, active-time calculations, live counts, and a live agent's status or prompt-safe activity text. Workflow prompts are never exposed by these APIs. A run is visible only through the session that owns it.

## DTS execution (SDK draft)

The experimental `@github/copilot-sdk/workflow-dts` entry point adds a Durable
Task Scheduler (DTS) execution provider. It preserves `defineWorkflow` and
ordinary `async run(ctx)` bodies, including the published `review-changed`
example, with a deliberately restricted effects profile. Omitting the provider preserves all existing local
workflow behavior described above.

> [!WARNING]
> This is an SDK execution slice, not a shipped Copilot managed workflow service.
> Native CLI authoring, automatic reconnection, pause/cancel mapping, and
> resource-limit accounting require runtime integration. The existing CLI
> authoring tool does not automatically select this provider.

Workflow intent does not name a scheduler resource, endpoint, or connection
string. The owning Copilot runtime or headless host supplies already configured
DTS client/worker objects and an execution host. Copilot can supply those
bindings from a managed service in the future without changing author metadata.
This draft does not provision Azure resources or implement that service.

### Authoring and host binding

An existing `.mjs` workflow can use awaited agents, journaled steps, and
top-level `ctx.parallel()`:

```js
import { defineWorkflow } from "@github/copilot-sdk/extension";

export const review = defineWorkflow({
    meta: {
        name: "durable-review",
        description: "Inspect, then verify a review.",
        phases: [],
    },
    run: async (ctx) => {
        const finding = await ctx.agent("Inspect the change", { label: "inspect" });
        const report = await ctx.step("report-v1", () => ({ finding }));
        const verified = await ctx.agent(JSON.stringify(report), { label: "verify" });
        return { report, verified };
    },
});
```

The **host**, not the author, constructs the executor. In this host-side example,
`client`, `worker`, and `executionHost` are supplied bindings, and `codeVersion`
identifies the immutable artifact containing the workflow and its dependencies:

```js
import { DtsWorkflowExecutor } from "@github/copilot-sdk/workflow-dts";

export async function runInHeadlessHost(
    { client, worker, executionHost, codeVersion },
    invocation
) {
    const executor = new DtsWorkflowExecutor({
        client,
        worker,
        executionHost,
        codeVersion,
        workflows: [review],
    });
    await worker.start();
    try {
        // invocation: { scopeId, runId, args }
        return await executor.run(review, invocation);
    } finally {
        await worker.stop();
        await client.stop();
    }
}
```

`scopeId` is the stable owning session/service scope. `runId` identifies the
durable run within it. Reusing both attaches to the existing scheduler instance;
changed arguments, workflow code/metadata, artifact identity, or Node runtime
version reject rather than starting a different run. The provider also pins its
driver protocol and Durable Task SDK version. Keep the complete artifact
unchanged until its runs settle. A function-source hash cannot validate imported
helpers or module state: the host must pin those dependencies with `codeVersion`.

`WorkflowExecutionHost` dispatches effects:

```ts
import type {
    JsonValue,
    WorkflowActivityInvocation,
    WorkflowAgentOptions,
    WorkflowInvocation,
    WorkflowProgressEvent,
} from "@github/copilot-sdk";

interface WorkflowExecutionHost {
    agent(
        invocation: WorkflowActivityInvocation,
        prompt: string,
        options: WorkflowAgentOptions,
    ): Promise<JsonValue>;
    step(
        invocation: WorkflowActivityInvocation,
        key: string,
        producer: () => JsonValue | Promise<JsonValue>,
    ): Promise<JsonValue>;
    progress?(
        invocation: WorkflowInvocation,
        event: WorkflowProgressEvent,
    ): Promise<void>;
}
```

Activity invocations carry `{ scopeId, runId, args, operationId }`. Use
`operationId` for host-side idempotency. An agent host returns JSON, including
`null` for an ordinary agent failure, and rejects for a hard runtime failure.
A local step host executes `producer()` and returns its JSON result. A future
sandbox/cloud host must load the pinned artifact and reconstruct the operation
there, not serialize the closure. Scheduler history and execution location are
separate responsibilities.

Progress events carry a stable `eventId`, ordered `sequence`, `kind` (`phase`
or `log`), and `text`. A workflow using phases or logs requires a progress sink;
omitting it fails explicitly. Deduplicate redelivery by `eventId` in that sink.
The native session binding sends progress with the current execution token and
stable sequence; a managed cross-process sink still needs durable idempotency.
This is not an exactly-once guarantee for an arbitrary external sink.

The existing extension registration path also accepts
`joinSession({ workflows: [review], workflowExecutor: executor })`. The binding
stays local to the joining SDK connection; it is stripped from wire metadata.
Each dispatched agent uses the currently bound session execution token. Tokens,
session objects, credentials, and closures are never stored in DTS history.
This contribution binding does not by itself implement automatic native
CLI recovery or durable lifecycle controls.

### Replay and activity semantics

The deterministic DTS orchestrator schedules an **Advance** activity, which
replays the user's async body outside the orchestrator. Settled `ctx` operations
validate their ordered command identities and return cloned recorded outcomes.
The first unseen operation exposes a JSON command out of band and returns an
unresolved promise to the body. Advance returns the command to DTS without
throwing a suspension exception through the author's `catch` or `finally`.
DTS then schedules a separate **ExecuteAgent** or **ExecuteStep** activity and
records its outcome before scheduling the next Advance.

For `ctx.parallel(thunks)`, Advance discovers one effect frontier per branch.
DTS schedules all distinct frontier activities before awaiting any completion;
it does not execute the thunks serially inside a long activity. The orchestrator
records actual completion order and per-branch histories while preserving input
array order in the returned results. Replaying branch promises uses that
recorded completion order. A recorded rejection enters its originating branch's
`catch` before the adapter decides whether it remains an unhandled failure.
Completed parallel groups replay their branch continuations before returning the
aggregate to the root, so pure workflow-local captures are rebuilt for later
step producers; this replay never reschedules their recorded effects.
A branch can contain sequential agent/step calls;
this profile drains a frontier round before discovering the next round, so it
does not implement the barrier-free `pipeline` helper. Duplicate agent calls or
step keys share one scheduled effect, including within a parallel round.

`ctx.phase()` and `ctx.log()` remain void-returning author APIs. Advance collects
their ordered metadata without emitting it during replay. Separate **Progress**
activities deliver new records before the next effects, and deliver the final
log before the run completes. Root progress is replay-validated and durable;
an unhandled root failure also carries its collected progress back to the
orchestrator, which delivers new phase/log records before failing the run.
progress from competing branches or inside step producers remains unsupported.

ExecuteStep replays the same prefix to recreate the producer at its operation
locator. It captures prior recorded JSON values again; it does not persist a
JavaScript heap. Captures must be reproducible from immutable code, invocation
arguments, and prior results. Mutable module/global state, an old session object,
or an external live handle is not a durable capture.

Agent memoization uses the canonical prompt and supported options, including
`label`. Identical sequential calls reuse their fulfilled outcome, including
`null`. Distinct labels remain independent. A step's key remains its sole
memoization identity, even when a later same-key producer differs. Version keys
when their meaning changes. Rejected effects are recorded and replayed as
rejections, not converted to `null` or memoized as successful results.
JSON-RPC failures retain their name, message, numeric code, and JSON data.
Other errors retain name/message, not custom prototypes or stacks; JSON-valued
rejections retain their value.
An ordinary thrown parallel-branch error becomes `null`; hard JSON-RPC,
connection, and cancellation failures propagate. Already-admitted parallel
activities are drained before propagation rather than silently orphaned.

Activities are **at-least-once**, not exactly-once side effects. Losing a worker
after an effect but before DTS acknowledges its completion can repeat it.
Completed recorded activities are not re-executed during ordinary recovery.
Keep side effects idempotent. History stores prompts, inputs, results, and
failure text; the owning service must enforce access control and retention.
This draft does not provide tenant isolation.

### Supported subset and lifecycle boundaries

Awaited `ctx.agent()`, journaled `ctx.step()`, top-level `ctx.parallel()`, and
root `ctx.phase()`/`ctx.log()` are supported. `ctx.pipeline`, `ctx.pause`, nested
parallel/workflows, `ctx.session`, volatile steps, and effects nested inside producers
fail explicitly. Concurrent/unawaited SDK calls and competing SDK calls through
raw `Promise.all`/`Promise.race` are rejected; arbitrary JavaScript race semantics
are not implemented. Profile violations cannot be made valid by catching them.
Declared resource limits reject registration until native accounting is wired.
The v3 driver/activity protocol pins these history semantics. It does not
replay an existing v1/v2 instance as v3: an incompatible instance/artifact rejects.

The body outside producers must be pure and deterministic. Put I/O in a step
producer or the execution host. Replay diagnoses new async resources and waits
that never reach an SDK boundary. This is **not a sandbox**: synchronous I/O,
clock/random reads, mutations, and all possible promise misuse cannot be
prevented or completely detected. Invalid raw I/O can begin before a diagnostic
is raised. Those operations are unsupported even when no diagnostic fires.
An infinite CPU/microtask loop is not contained by this driver.
`ctx.signal` is available only inside a producer and uses the live binding's
signal when present, never a signal restored from history.

Stopping a worker leaves scheduler history intact. A cold worker with the same
artifact and a fresh `executionHost` resumes incomplete activities. Aborting
`executor.run` promptly rejects that caller's wait, including during a pending
scheduler lookup, admission, or poll, and releases its live binding. Late RPC
results cannot complete the cancelled waiter or remove a replacement binding.
An already dispatched admission can still reach the scheduler after cancellation;
reattach using the same scope/run IDs rather than assuming no run was created.
Cancellation does not terminate DTS. Scheduler
termination/suspension, Copilot pause/cancel outcomes, budget enforcement, and
native session-token rebinding need a runtime/service owner before this binding
can be shipped through native authoring tools. Unsupported operations do not
fall back to the local journal.

### Developer restart proof

The provider requires Node.js 22 or later and the optional peer
`@microsoft/durabletask-js@0.4.0`. Local-only installations do not install this
peer and retain the SDK's Node.js 20.19 minimum, including engine-strict installs.
The host opting into DTS installs the peer, not a workflow author:

```powershell
npm install @microsoft/durabletask-js@0.4.0 --registry=https://packagefeedproxy.microsoft.io/npm/
```

SDK development installs the pinned DTS package as a development dependency.
The explicit DTS test profile requires a responsive Docker engine
and the Microsoft DTS emulator. From the SDK root on Windows:

```powershell
npm --prefix .\nodejs ci --ignore-scripts --registry=https://packagefeedproxy.microsoft.io/npm/
npm --prefix .\nodejs run test:workflow-dts
```

The separate minimum-runtime regression builds/packs the SDK, installs that
archive with `--engine-strict` using Node.js 20.19.0 in an isolated consumer,
and exercises the default Local workflow and ESM/CommonJS exports without DTS.
It then explicitly installs the optional peer with the development host's
Node.js 22+ runtime and checks the packed DTS provider's ESM/CommonJS exports.
Supply an existing Node.js 20.19.0 binary; this check does not install runtimes:

```powershell
npm --prefix .\nodejs run test:workflow-node20 -- "C:\toolchains\node20\node.exe"
```

The tests create uniquely named emulator containers with loopback-only
dynamic ports. It uses the `default` task hub and no authentication, and records
the pinned image digest in its proof output. No Azure resources are provisioned.
It fails if Docker/emulator execution is unavailable; it does not substitute a
mock backend. The default Node suite does not start this container.

The sequential scenario completes an agent activity and enters a second one. The test verifies
the first completion in real DTS history, kills only worker A's owned PID,
and starts an independently loaded worker B against the still-running scheduler.
It asserts the same instance ID, exactly one execution of the first activity,
redelivery of the incomplete second activity with the same operation ID and
prior value, reconstruction of the report step, and exact final JSON output.
It cleans up only its workers, container, and observation directory.
The host is a deterministic fixture, not a live Copilot model. Separate in-memory
tests cover replay/dispatch semantics; they are not restart evidence.
This proves worker recovery, not scheduler-container restart or disk-loss
recovery.

The published-example scenario loads
[`samples/review-changed.mjs`](../samples/review-changed.mjs) with the exact
metadata and body from the first example above. Two gated, distinct agent
activities must both enter before either finishes. The later input branch
finishes first; a cold worker then resumes the other branch without rerunning
the recorded one. The test asserts completion order, input-order results,
report-producer reconstruction, both phase records, and the final log.
The gates are test-only execution-host behavior, not a change to author code.
An additional capture scenario verifies that recorded parallel continuations
rebuild workflow-local values before a post-restart report producer executes,
without repeating the recorded agent activity.
For this published example, the **Verify** phase contains a report step;
it is not an additional AI verification call.

To opt the unchanged example into a native extension, import both the sample
and `joinSession` from the same built SDK module graph, construct the
runtime-owned `DtsWorkflowExecutor`, and call:

```js
const session = await joinSession({
    workflows: [reviewChanged],
    workflowExecutor: executor,
});
```

The scoped developer demonstration uses that native workflow callback path
and a local emulator. It does not change the installed CLI's authoring tool,
or make DTS the default backend. A headless execution host is still an option;
its model/session lifecycle must be distinguished from native workflow agents.

Before release, reconcile this standalone SDK draft into the runtime
repository's `src/sdk`; the runtime release snapshot owns the published tree.
