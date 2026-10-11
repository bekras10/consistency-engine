/**
 * Compare `npm audit` in apps/web with apps/web/npm-audit-allowlist.json.
 *
 * Production (`npm audit --omit=dev`) must report no advisories. The full
 * audit's advisory ids must match the allowlist exactly. A new critical
 * production advisory fails because it is not listed and because production
 * must be empty.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const web = resolve(root, "apps/web");
const allowPath = resolve(web, "npm-audit-allowlist.json");

function runAudit(omitDev) {
  const args = ["audit", "--json"];
  if (omitDev) args.push("--omit=dev");
  let stdout = "";
  try {
    stdout = execFileSync("npm", args, { cwd: web, encoding: "utf8" });
  } catch (error) {
    stdout = typeof error.stdout === "string" ? error.stdout : "";
    if (!stdout) throw error;
  }
  return JSON.parse(stdout);
}

function advisories(report) {
  const found = new Map();
  for (const [pkg, info] of Object.entries(report.vulnerabilities ?? {})) {
    const vias = Array.isArray(info.via) ? info.via : [];
    for (const via of vias) {
      if (typeof via !== "object" || via === null || typeof via.url !== "string") continue;
      const match = via.url.match(/GHSA-[0-9a-z-]+/i);
      if (!match) continue;
      const id = match[0];
      const row = found.get(id) ?? {
        id,
        severity: typeof via.severity === "string" ? via.severity : String(info.severity),
        title: typeof via.title === "string" ? via.title : pkg,
        packages: new Set(),
      };
      row.packages.add(pkg);
      if (typeof via.name === "string") row.packages.add(via.name);
      found.set(id, row);
    }
  }
  return found;
}

const allow = JSON.parse(readFileSync(allowPath, "utf8"));
const listed = new Map();
for (const item of allow.advisories ?? []) {
  if (typeof item.id !== "string" || !item.id.startsWith("GHSA-")) {
    throw new Error("allowlist entry is missing a GHSA id");
  }
  if (item.scope !== "dev" && item.scope !== "prod") {
    throw new Error(`${item.id} must set scope to dev or prod`);
  }
  if (item.scope === "prod" && item.severity === "critical") {
    throw new Error(`${item.id} is a critical production advisory and cannot be allowlisted`);
  }
  listed.set(item.id, item);
}

const full = advisories(runAudit(false));
const prod = advisories(runAudit(true));
const errors = [];

for (const [id, row] of prod) {
  errors.push(
    `production advisory ${id} (${row.severity}, ${[...row.packages].sort().join(", ")}) is not allowed`,
  );
}

for (const [id, row] of full) {
  if (!listed.has(id)) {
    errors.push(
      `advisory ${id} (${row.severity}: ${row.title}) is not in npm-audit-allowlist.json`,
    );
  }
}
for (const [id, item] of listed) {
  if (!full.has(id)) {
    errors.push(`allowlist entry ${id} is no longer reported by npm audit`);
  } else if (item.scope === "prod") {
    errors.push(`${id} is marked prod in the allowlist; production audit must stay empty`);
  }
}

const counts = full.size === 0 ? "no advisories" : `${full.size} advisory id(s)`;
console.log(`npm audit advisories: ${counts}; production advisories: ${prod.size}.`);
for (const row of full.values()) {
  console.log(
    `  ${row.id} ${row.severity} packages=${[...row.packages].sort().join(",")}`,
  );
}
if (errors.length > 0) {
  for (const line of errors) console.error(line);
  process.exit(1);
}
console.log("npm audit matches the allowlist. Production audit is empty.");
