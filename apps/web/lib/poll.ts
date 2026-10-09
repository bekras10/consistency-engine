import type { Envelope } from "@/lib/types";

export type PollConnection = "connected" | "stale" | "error";

export type PollSnapshot<T> = {
  data: T;
  initial: T;
  error: string | null;
  status: PollConnection;
  hasSuccess: boolean;
};

export function reducePoll<T>(previous: PollSnapshot<T>, envelope: Envelope<T>): PollSnapshot<T> {
  if (envelope.ok) {
    return {
      data: envelope.data,
      initial: previous.initial,
      error: null,
      status: "connected",
      hasSuccess: true,
    };
  }
  if (previous.hasSuccess) {
    return {
      data: previous.data,
      initial: previous.initial,
      error: envelope.error,
      status: "stale",
      hasSuccess: true,
    };
  }
  return {
    data: previous.data,
    initial: previous.initial,
    error: envelope.error,
    status: "error",
    hasSuccess: false,
  };
}
