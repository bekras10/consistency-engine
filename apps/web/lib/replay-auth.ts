import { timingSafeEqual } from "node:crypto";

/** Server-side replay token. Computed access so Next does not inline a build-time empty value. */

export function replayMutationsPublic(): boolean {
  const flag = readEnv("REPLAY_MUTATIONS_PUBLIC").trim().toLowerCase();
  return flag === "1" || flag === "true" || flag === "yes";
}

export function expectedReplayToken(): string {
  return readEnv("REPLAY_API_TOKEN").trim();
}

export function replayTokenAccepted(presented: string): boolean {
  if (replayMutationsPublic()) return true;
  const expected = expectedReplayToken();
  if (!expected || !presented || expected.length !== presented.length) return false;
  const left = Buffer.from(expected);
  const right = Buffer.from(presented);
  if (left.length !== right.length) return false;
  return timingSafeEqual(left, right);
}

function readEnv(name: string): string {
  const value = process.env[name];
  return typeof value === "string" ? value : "";
}
