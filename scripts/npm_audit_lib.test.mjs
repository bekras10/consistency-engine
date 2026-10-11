import assert from "node:assert/strict";
import test from "node:test";

import { evaluateAudit } from "./npm_audit_lib.mjs";

const empty = {
  vulnerabilities: {},
  metadata: { vulnerabilities: { info: 0, low: 0, moderate: 0, high: 0, critical: 0, total: 0 } },
};

test("a production vulnerability count above zero fails without a GHSA id", () => {
  const prod = {
    vulnerabilities: {
      postcss: { severity: "moderate", via: ["next"] },
    },
    metadata: {
      vulnerabilities: { info: 0, low: 0, moderate: 2, high: 0, critical: 0, total: 2 },
    },
  };
  const errors = evaluateAudit(empty, prod, { advisories: [] });
  assert.ok(errors.length > 0);
  assert.ok(errors.some((line) => /production/i.test(line)));
});

test("a finding with no GHSA id still counts as an unknown advisory format", () => {
  const full = {
    advisories: {
      111: { title: "no ghsa", severity: "high", module_name: "left-pad" },
    },
  };
  const errors = evaluateAudit(full, empty, { advisories: [] });
  assert.ok(errors.length > 0);
  assert.ok(errors.some((line) => /unknown|left-pad|no GHSA|GHSA/i.test(line)));
});

test("an allowlisted id that npm no longer reports still fails", () => {
  const errors = evaluateAudit(empty, empty, {
    advisories: [{ id: "GHSA-vfj7-8cjw-p6xm", scope: "dev", severity: "high" }],
  });
  assert.ok(errors.some((line) => line.includes("GHSA-vfj7-8cjw-p6xm")));
  assert.ok(errors.some((line) => /no longer reported/.test(line)));
});

test("the dev allowlist matches when production is empty", () => {
  const via = {
    url: "https://github.com/advisories/GHSA-vfj7-8cjw-p6xm",
    severity: "high",
    title: "braces",
    name: "braces",
  };
  const full = {
    vulnerabilities: { braces: { severity: "high", via: [via] } },
    metadata: {
      vulnerabilities: { info: 0, low: 0, moderate: 0, high: 1, critical: 0, total: 1 },
    },
  };
  const errors = evaluateAudit(full, empty, {
    advisories: [{ id: "GHSA-vfj7-8cjw-p6xm", scope: "dev", severity: "high" }],
  });
  assert.deepEqual(errors, []);
});
