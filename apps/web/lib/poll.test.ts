import { describe, expect, it } from "vitest";

import { reducePoll, type PollSnapshot } from "./poll";

describe("usePolled error envelope", () => {
  it("keeps the last successful poll and reports the error", () => {
    const initial = { value: "initial" };
    const success = { value: "last-success" };
    const start: PollSnapshot<typeof initial> = {
      data: initial,
      initial,
      error: null,
      status: "connected",
      hasSuccess: false,
    };
    const afterSuccess = reducePoll(start, { ok: true, data: success });
    const afterError = reducePoll(afterSuccess, { ok: false, error: "database_unavailable" });
    expect(afterError.data).toEqual(success);
    expect(afterError.data).not.toEqual(initial);
    expect(afterError.status).toBe("stale");
    expect(afterError.error).toBe("database_unavailable");
  });
});
