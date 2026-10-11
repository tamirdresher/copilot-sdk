import { defineConfig } from "vitest/config";

export default defineConfig({
    test: {
        include: ["test/integration/workflow-dts.test.ts"],
        environment: "node",
        testTimeout: 180_000,
        hookTimeout: 30_000,
    },
});
