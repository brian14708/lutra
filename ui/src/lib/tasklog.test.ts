import { expect, test } from "vitest";
import { encode, Tagged } from "cborg";
import { decodeTaskLog } from "@/lib/tasklog.ts";

test("resolves a stored task log envelope", async () => {
  const event = {
    type: "task.log.v1",
    source: "stderr",
    message: "x".repeat(65536),
    timestamp: "2026-09-30T00:00:00Z",
    action_id: "00000000-0000-0000-0000-000000000001",
    attempt: 1,
    phase: "task",
  };
  const uri = "blob:application/cbor,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
  const record = encode(new Tagged(32, uri.replace(",", ";resolve,")));
  expect(
    await decodeTaskLog(record, async (requested) => {
      expect(requested).toBe(uri);
      return encode(event);
    }),
  ).toEqual(event);
});

test("decodes a task log envelope", async () => {
  const event = {
    type: "task.log.v1",
    source: "stderr",
    message: "hello",
    timestamp: "2026-09-30T00:00:00Z",
    action_id: "00000000-0000-0000-0000-000000000001",
    attempt: 1,
    phase: "task",
  };
  expect(await decodeTaskLog(encode(event))).toEqual(event);
});

test("rejects unknown task log envelopes", async () => {
  await expect(decodeTaskLog(encode({ type: "other" }))).rejects.toThrow(/invalid task log/);
});

test.each(["pull", "build"])(
  "decodes %s metadata and rejects incomplete image logs",
  async (phase) => {
    const event = {
      type: "task.log.v1",
      source: "stderr",
      phase,
      runtime: "podman",
      image: "docker.io/library/python:3.12-slim",
      message: "copying layer",
      timestamp: "2026-09-30T00:00:00Z",
      action_id: "00000000-0000-0000-0000-000000000001",
      attempt: 1,
    };
    expect(await decodeTaskLog(encode(event))).toEqual(event);
    await expect(decodeTaskLog(encode({ ...event, runtime: "other" }))).rejects.toThrow(/invalid/);
    await expect(decodeTaskLog(encode({ ...event, image: "" }))).rejects.toThrow(/invalid/);
    await expect(decodeTaskLog(encode({ ...event, phase: "task" }))).rejects.toThrow(/invalid/);
  },
);

test("rejects malformed timestamps and action IDs", async () => {
  const event = {
    type: "task.log.v1",
    source: "stderr",
    message: "hello",
    timestamp: "2026-09-30T00:00:00Z",
    action_id: "00000000-0000-0000-0000-000000000001",
    attempt: 1,
    phase: "task",
  };
  await expect(decodeTaskLog(encode({ ...event, timestamp: "invalid" }))).rejects.toThrow(
    /invalid/,
  );
  await expect(decodeTaskLog(encode({ ...event, action_id: "invalid" }))).rejects.toThrow(
    /invalid/,
  );
});
