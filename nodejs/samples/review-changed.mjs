/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

// The published workflow is unchanged; only its SDK import/registration is host-owned.
import { defineWorkflow } from "../dist/extension.js";

export const reviewChanged = defineWorkflow({
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
