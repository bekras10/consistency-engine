/**
 * Decision logic for `npm audit` output.
 *
 * The CLI in check_npm_audit.mjs loads the real reports. Tests call
 * `evaluateAudit` with fixture JSON. A production metadata count above zero
 * fails. A finding that does not carry a GHSA id still counts. An allowlisted
 * id that the report no longer contains fails.
 */

function ghsaFromUrl(url) {
  if (typeof url !== "string") return "";
  const match = url.match(/GHSA-[0-9a-z-]+/i);
  return match ? match[0] : "";
}

function viaList(info) {
  return Array.isArray(info?.via) ? info.via : [];
}

function ghsaFromVia(info) {
  const ids = [];
  for (const via of viaList(info)) {
    if (typeof via !== "object" || via === null) continue;
    const id = ghsaFromUrl(via.url);
    if (id) ids.push(id);
  }
  return ids;
}

function stringVias(info) {
  return viaList(info).filter((via) => typeof via === "string");
}

function objectViaWithoutGhsa(info) {
  return viaList(info).some((via) => {
    if (typeof via !== "object" || via === null) return false;
    return ghsaFromUrl(via.url) === "";
  });
}

export function advisories(report) {
  const found = new Map();
  const vulns = report?.vulnerabilities;
  if (!vulns || typeof vulns !== "object" || Array.isArray(vulns)) return found;
  for (const [pkg, info] of Object.entries(vulns)) {
    for (const via of viaList(info)) {
      if (typeof via !== "object" || via === null) continue;
      const id = ghsaFromUrl(via.url);
      if (!id) continue;
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

function reachesGhsa(vulns, pkg, seen) {
  if (seen.has(pkg)) return false;
  seen.add(pkg);
  const info = vulns[pkg];
  if (!info || typeof info !== "object") return false;
  if (ghsaFromVia(info).length > 0) return true;
  for (const name of stringVias(info)) {
    if (reachesGhsa(vulns, name, seen)) return true;
  }
  return false;
}

function rowFor(id, info, pkg) {
  return {
    id,
    severity: typeof info?.severity === "string" ? info.severity : "unknown",
    title: typeof info?.title === "string" ? info.title : pkg,
    packages: new Set(pkg ? [pkg] : []),
  };
}

function collect(report) {
  const ids = new Map();
  let unidentified = 0;
  const vulns = report?.vulnerabilities;
  if (vulns && typeof vulns === "object" && !Array.isArray(vulns)) {
    const parsed = advisories(report);
    for (const [id, row] of parsed) ids.set(id, row);
    for (const [pkg, info] of Object.entries(vulns)) {
      if (ghsaFromVia(info).length > 0) continue;
      const inherited = !objectViaWithoutGhsa(info) && reachesGhsa(vulns, pkg, new Set());
      if (!inherited) unidentified += 1;
    }
    return { ids, unidentified };
  }
  const block = report?.advisories;
  if (block && typeof block === "object" && !Array.isArray(block)) {
    for (const [key, info] of Object.entries(block)) {
      const record = info && typeof info === "object" ? info : {};
      const id =
        ghsaFromUrl(record.url) ||
        (typeof record.github_advisory_id === "string" && record.github_advisory_id.startsWith("GHSA-")
          ? record.github_advisory_id
          : "") ||
        (typeof record.id === "string" && record.id.startsWith("GHSA-") ? record.id : "");
      const pkg = typeof record.module_name === "string" ? record.module_name : key;
      if (!id) {
        unidentified += 1;
        continue;
      }
      const row = ids.get(id) ?? rowFor(id, record, pkg);
      row.packages.add(pkg);
      ids.set(id, row);
    }
  }
  return { ids, unidentified };
}

export function vulnerabilityTotal(report) {
  const meta = report?.metadata?.vulnerabilities;
  if (!meta || typeof meta !== "object") return null;
  if (typeof meta.total === "number" && Number.isFinite(meta.total)) return meta.total;
  const keys = ["info", "low", "moderate", "high", "critical"];
  if (keys.every((key) => typeof meta[key] === "number")) {
    return keys.reduce((sum, key) => sum + meta[key], 0);
  }
  return null;
}

function knownShape(report) {
  if (!report || typeof report !== "object" || Array.isArray(report)) return false;
  const vulns = report.vulnerabilities;
  const advisoriesBlock = report.advisories;
  const meta = report.metadata?.vulnerabilities;
  return Boolean(
    (vulns && typeof vulns === "object") ||
      (advisoriesBlock && typeof advisoriesBlock === "object") ||
      (meta && typeof meta === "object"),
  );
}

function allowlist(allow) {
  const listed = new Map();
  for (const item of allow?.advisories ?? []) {
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
  return listed;
}

/** @returns {string[]} errors. Empty means the reports match the allowlist. */
export function evaluateAudit(fullReport, prodReport, allow) {
  const listed = allowlist(allow);
  const errors = [];
  if (!knownShape(fullReport)) {
    errors.push("full npm audit JSON is an unknown advisory format");
  }
  if (!knownShape(prodReport)) {
    errors.push("production npm audit JSON is an unknown advisory format");
  }
  const full = collect(fullReport);
  const prod = collect(prodReport);
  const prodTotal = vulnerabilityTotal(prodReport);
  if ((prodTotal ?? 0) > 0 || prod.unidentified > 0 || prod.ids.size > 0) {
    const count = Math.max(prodTotal ?? 0, prod.ids.size + prod.unidentified);
    errors.push(`production vulnerability count is ${count}`);
  }
  if (full.unidentified > 0) {
    errors.push(
      `npm audit reported ${full.unidentified} finding(s) with no GHSA id; unknown advisory format still counts`,
    );
  }
  for (const [id, row] of prod.ids) {
    errors.push(
      `production advisory ${id} (${row.severity}, ${[...row.packages].sort().join(", ")}) is not allowed`,
    );
  }
  for (const [id, row] of full.ids) {
    if (!listed.has(id)) {
      errors.push(
        `advisory ${id} (${row.severity}: ${row.title}) is not in npm-audit-allowlist.json`,
      );
    }
  }
  for (const [id, item] of listed) {
    if (!full.ids.has(id)) {
      errors.push(`allowlist entry ${id} is no longer reported by npm audit`);
    } else if (item.scope === "prod") {
      errors.push(`${id} is marked prod in the allowlist; production audit must stay empty`);
    }
  }
  return errors;
}
