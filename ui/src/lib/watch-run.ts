import type { Client } from "@connectrpc/connect";
import type { LutraService, WatchRunResponse } from "@/proto/lutra/v1/lutra_pb";

export async function* watchRun({
  client,
  runId,
  keyPrefix,
  signal,
  onDisconnect,
}: {
  client: Pick<Client<typeof LutraService>, "watchRun">;
  runId: string;
  keyPrefix: Uint8Array;
  signal: AbortSignal;
  onDisconnect: (error: unknown) => void;
}): AsyncGenerator<WatchRunResponse> {
  let cursor = 0n;
  let terminal = false;
  while (!signal.aborted) {
    try {
      for await (const update of client.watchRun(
        { id: runId, taskLogs: { stream: "task_log", afterSeq: cursor, keyPrefix } },
        { signal },
      )) {
        if (signal.aborted) return;
        if (update.run) {
          terminal = ["succeeded", "failed", "canceled"].includes(update.run.status);
        }
        if (update.log) {
          if (update.log.seq <= cursor) continue;
          cursor = update.log.seq;
        }
        yield update;
      }
      if (terminal) return;
      throw new Error("Run stream ended before completion");
    } catch (error: unknown) {
      if (signal.aborted) return;
      if (terminal) throw error;
      onDisconnect(error);
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer);
          signal.removeEventListener("abort", done);
          resolve();
        };
        const timer = setTimeout(done, 500);
        signal.addEventListener("abort", done, { once: true });
      });
    }
  }
}
