import { expect, test } from "vitest";
import { encode } from "cborg";
import { decodeTaskLog } from "@/lib/tasklog.ts";

test("decodes a task log envelope", () => {
  expect(
    decodeTaskLog(
      encode({
        type: "task.log.v1",
        source: "stderr",
        message: "hello",
        timestamp: "2026-09-30T00:00:00Z",
        action_id: "00000000-0000-0000-0000-000000000001",
        attempt: 1,
      }),
    ),
  ).toMatchObject({ message: "hello", attempt: 1 });
});

test("rejects unknown task log envelopes", () => {
  expect(() => decodeTaskLog(encode({ type: "other" }))).toThrow(/invalid task log/);
});

test("rejects malformed timestamps and action IDs", () => {
  const event = {
    type: "task.log.v1",
    source: "stderr",
    message: "hello",
    timestamp: "2026-09-30T00:00:00Z",
    action_id: "00000000-0000-0000-0000-000000000001",
    attempt: 1,
  };
  expect(() => decodeTaskLog(encode({ ...event, timestamp: "invalid" }))).toThrow(/invalid/);
  expect(() => decodeTaskLog(encode({ ...event, action_id: "invalid" }))).toThrow(/invalid/);
});
