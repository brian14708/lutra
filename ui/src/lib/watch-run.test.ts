import { afterEach, expect, test, vi } from "vitest";
import { create } from "@bufbuild/protobuf";
import { WatchRunResponseSchema } from "@/proto/lutra/v1/lutra_pb";
import { watchRun } from "@/lib/watch-run";

afterEach(() => vi.useRealTimers());

test("resumes WatchRun with the last sequence and action prefix", async () => {
  vi.useFakeTimers();
  let calls = 0;
  const disconnected = vi.fn();
  const updates = watchRun({
    client: {
      async *watchRun(request) {
        calls++;
        expect(request.id).toBe("run");
        expect(request.taskLogs?.keyPrefix).toEqual(new TextEncoder().encode("action"));
        expect(request.taskLogs?.afterSeq).toBe(calls === 1 ? 0n : 7n);
        if (calls === 1) {
          yield create(WatchRunResponseSchema, { log: { seq: 7n } });
          throw new Error("connection lost");
        }
        yield create(WatchRunResponseSchema, { run: { status: "succeeded" } });
        yield create(WatchRunResponseSchema, { log: { seq: 8n } });
      },
    },
    runId: "run",
    keyPrefix: new TextEncoder().encode("action"),
    signal: new AbortController().signal,
    onDisconnect: disconnected,
  });
  expect((await updates.next()).value?.log?.seq).toBe(7n);
  const resumed = updates.next();
  await vi.advanceTimersByTimeAsync(500);
  expect((await resumed).value?.run?.status).toBe("succeeded");
  expect((await updates.next()).value?.log?.seq).toBe(8n);
  expect((await updates.next()).done).toBe(true);
  expect(calls).toBe(2);
  expect(disconnected).toHaveBeenCalledOnce();
});

test("does not reconnect after terminal status and reports a failed final replay", async () => {
  const disconnected = vi.fn();
  const updates = watchRun({
    client: {
      async *watchRun() {
        yield create(WatchRunResponseSchema, { run: { status: "failed" } });
        throw new Error("final replay failed");
      },
    },
    runId: "run",
    keyPrefix: new Uint8Array(),
    signal: new AbortController().signal,
    onDisconnect: disconnected,
  });
  expect((await updates.next()).value?.run?.status).toBe("failed");
  await expect(updates.next()).rejects.toThrow("final replay failed");
  expect(disconnected).not.toHaveBeenCalled();
});
