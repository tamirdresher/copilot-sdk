import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
    assertExactProductionDependencies,
    assertMinimumPublicationAge,
    loadNpmPublicationTimes,
    readProductionDependencyManifest,
    readResolvedProductionDependencies,
    requiresPublicationCooldown,
} from "./dependency-policy.js";

async function main(): Promise<void> {
    const nodeRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
    const manifest = readProductionDependencyManifest(join(nodeRoot, "package.json"));
    const directDependencies = assertExactProductionDependencies(manifest);
    const resolvedDependencies = readResolvedProductionDependencies(
        join(nodeRoot, "package-lock.json")
    );
    const publicationDependencies = [
        ...new Map(
            [...directDependencies, ...resolvedDependencies].map((dependency) => [
                `${dependency.name}@${dependency.version}`,
                dependency,
            ])
        ).values(),
    ];
    const publicationTimes = await loadNpmPublicationTimes(publicationDependencies);
    assertMinimumPublicationAge(publicationDependencies, publicationTimes);
    const externalCount = publicationDependencies.filter((dependency) =>
        requiresPublicationCooldown(dependency.name)
    ).length;
    console.log(
        `Verified ${directDependencies.length} exact direct production/peer dependencies; ${externalCount} production/peer package versions meet the seven-day npm publication age requirement.`
    );
}

main().catch((error) => {
    console.error(`::error::${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
});
