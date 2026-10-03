import { decode } from "@/lib/value";

type TaskLogFields = {
  type: "task.log.v1";
  source: "stderr";
  message: string;
  timestamp: string;
  action_id: string;
  attempt: number;
};

export type TaskLogEvent = TaskLogFields &
  ({ phase: "task" } | { phase: "pull" | "build"; runtime: "docker" | "podman"; image: string });

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export async function decodeTaskLog(
  bytes: Uint8Array,
  resolver?: (uri: string) => Promise<Uint8Array>,
): Promise<TaskLogEvent> {
  const value = await decode(bytes, resolver);
  if (
    !isRecord(value) ||
    value.type !== "task.log.v1" ||
    value.source !== "stderr" ||
    typeof value.message !== "string" ||
    typeof value.timestamp !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$/.test(value.timestamp) ||
    !Number.isFinite(Date.parse(value.timestamp)) ||
    typeof value.action_id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value.action_id) ||
    typeof value.attempt !== "number" ||
    !Number.isInteger(value.attempt) ||
    value.attempt <= 0
  ) {
    throw new Error("invalid task log event");
  }
  const fields: TaskLogFields = {
    type: "task.log.v1",
    source: "stderr",
    message: value.message,
    timestamp: value.timestamp,
    action_id: value.action_id,
    attempt: value.attempt,
  };
  if (value.phase === "task" && value.runtime === undefined && value.image === undefined) {
    return { ...fields, phase: "task" };
  }
  if (
    (value.phase === "pull" || value.phase === "build") &&
    (value.runtime === "docker" || value.runtime === "podman") &&
    typeof value.image === "string" &&
    value.image.length > 0
  ) {
    return { ...fields, phase: value.phase, runtime: value.runtime, image: value.image };
  }
  throw new Error("invalid task log metadata");
}
