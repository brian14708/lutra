import { decode as decodeCbor } from "cborg";

export type TaskLogEvent = {
  type: "task.log.v1";
  source: "stderr";
  message: string;
  timestamp: string;
  action_id: string;
  attempt: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function decodeTaskLog(bytes: Uint8Array): TaskLogEvent {
  const value: unknown = decodeCbor(bytes);
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
  return {
    type: "task.log.v1",
    source: "stderr",
    message: value.message,
    timestamp: value.timestamp,
    action_id: value.action_id,
    attempt: value.attempt,
  };
}
