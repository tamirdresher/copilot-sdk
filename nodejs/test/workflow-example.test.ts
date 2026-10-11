/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import ts from "typescript";
import { expect, it } from "vitest";

function property(text: string, name: string): string {
    const file = ts.createSourceFile(
        "example.mjs",
        text,
        ts.ScriptTarget.Latest,
        true,
        ts.ScriptKind.JS
    );
    let value: string | undefined;
    const visit = (node: ts.Node): void => {
        if (ts.isPropertyAssignment(node) && node.name.getText(file) === name) {
            value = node.initializer.getText(file);
        }
        ts.forEachChild(node, visit);
    };
    visit(file);
    if (value === undefined) throw new Error(`Missing example ${name}`);
    return value;
}

it("keeps the shipped review-changed example's metadata and async body exactly unchanged", () => {
    const docs = readFileSync(new URL("../docs/workflows.md", import.meta.url), "utf8");
    const original = docs.match(/```js\r?\n([\s\S]*?)```/)?.[1];
    if (!original) throw new Error("Missing published workflow example");
    const sample = readFileSync(new URL("../samples/review-changed.mjs", import.meta.url), "utf8");
    expect(property(sample, "meta")).toBe(property(original, "meta"));
    expect(property(sample, "run")).toBe(property(original, "run"));
    expect(createHash("sha256").update(property(sample, "run")).digest("hex")).toBe(
        "1bf5988b7157241ad140ae61a9766328833742f786911382f705294aa7efb711"
    );
});
