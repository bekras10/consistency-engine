/**
 * Compare `npm audit` in apps/web with apps/web/npm-audit-allowlist.json.
 *
 * Production (`npm audit --omit=dev`) must report a vulnerability count of zero,
 * including a finding that has no GHSA id. The full audit's GHSA ids must match
 * the allowlist exactly. An unknown advisory JSON shape fails. A new critical
 * production advisory fails because it is not listed and because production
 * must be empty.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { advisories, evaluateAudit, vulnerabilityTotal } from "./npm_audit_lib.mjs";

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

const allow = JSON.parse(readFileSync(allowPath, "utf8"));
const fullReport = runAudit(false);
const prodReport = runAudit(true);
const full = advisories(fullReport);
const errors = evaluateAudit(fullReport, prodReport, allow);
const prodTotal = vulnerabilityTotal(prodReport);
const counts = full.size === 0 ? "no advisories" : `${full.size} advisory id(s)`;
const prodLabel = prodTotal === null ? "unknown" : String(prodTotal);
console.log(`npm audit advisories: ${counts}; production vulnerability count: ${prodLabel}.`);
for (const row of full.values()) {
  console.log(`  ${row.id} ${row.severity} packages=${[...row.packages].sort().join(",")}`);
}
if (errors.length > 0) {
  for (const line of errors) console.error(line);
  process.exit(1);
}
console.log("npm audit matches the allowlist. Production audit is empty.");
