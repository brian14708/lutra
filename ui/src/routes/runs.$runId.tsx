import { createClient } from "@connectrpc/connect";
import { createFileRoute, Link } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { LutraService, type Run } from "@/proto/lutra/v1/lutra_pb";
import { BlobService } from "@/proto/lutra/v1/blob_pb";
import { downloadBlob } from "@/lib/blob";
import { decodeTaskLog, type TaskLogEvent } from "@/lib/tasklog";
import { transport } from "@/lib/rpc";
import { watchRun } from "@/lib/watch-run";

export const Route = createFileRoute("/runs/$runId")({ component: RunDetail });
const runsRpc = createClient(LutraService, transport);
const blobsRpc = createClient(BlobService, transport);

type LogRow = { seq: bigint; key: Uint8Array; event: TaskLogEvent; created: number };

function RunDetail() {
  const { runId } = Route.useParams();
  const [run, setRun] = useState<Run>();
  const [rows, setRows] = useState<LogRow[]>([]);
  const [filter, setFilter] = useState("");
  const [state, setState] = useState("loading");
  const [error, setError] = useState<string>();

  useEffect(() => {
    setRun(undefined);
  }, [runId]);

  useEffect(() => {
    const controller = new AbortController();
    setRows([]);
    async function watch() {
      setState("loading");
      try {
        const prefix = filter ? new TextEncoder().encode(filter) : new Uint8Array();
        for await (const update of watchRun({
          client: runsRpc,
          runId,
          keyPrefix: prefix,
          signal: controller.signal,
          onDisconnect: (cause) => {
            setError(String(cause));
            setState("disconnected");
          },
        })) {
          if (controller.signal.aborted) return;
          setState("ready");
          setError(undefined);
          if (update.run) {
            setRun(update.run);
          }
          const response = update.log;
          if (!response) continue;
          try {
            const event = await decodeTaskLog(response.valueCbor, (uri) =>
              downloadBlob(blobsRpc, uri),
            );
            if (controller.signal.aborted) return;
            setRows((previous) => [
              ...previous,
              {
                seq: response.seq,
                key: response.key,
                event,
                created: Number(response.createdUnixNanos),
              },
            ]);
            setState("ready");
          } catch (cause: unknown) {
            if (controller.signal.aborted) return;
            setError(String(cause));
            setState("error");
          }
        }
      } catch (cause: unknown) {
        if (controller.signal.aborted) return;
        setError(String(cause));
        setState("error");
      }
    }
    void watch();
    return () => {
      controller.abort();
    };
  }, [runId, filter]);

  const visible = filter ? rows.filter((row) => row.event.action_id.startsWith(filter)) : rows;
  return (
    <main className="mx-auto max-w-[1050px] px-5 py-8 md:px-10 md:py-11">
      <Link to="/" className="text-[13px] font-semibold text-teal-700 hover:underline">
        Back to overview
      </Link>
      <div className="mt-5 mb-6">
        <p className="mb-2 text-[11px] font-bold tracking-widest text-teal-700 uppercase">Run</p>
        <h1 className="break-all text-[25px] font-bold">{runId}</h1>
      </div>
      <section className="rounded-lg border border-slate-200 bg-white p-5">
        <h2 className="text-base font-semibold">Status</h2>
        <p className="mt-2 text-[13px] text-slate-600">{run?.status ?? "Loading..."}</p>
        {run?.error && <p className="mt-2 text-[13px] text-rose-700">{run.error}</p>}
      </section>
      <section className="mt-5 rounded-lg border border-slate-200 bg-white p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-base font-semibold">Task logs</h2>
          <input
            aria-label="Action filter"
            value={filter}
            onChange={(event) => {
              setFilter(event.target.value);
            }}
            placeholder="Action ID prefix"
            className="rounded-md border border-slate-300 p-2 font-mono text-[12px]"
          />
        </div>
        <p className="mt-2 text-[12px] text-slate-500">
          {state === "disconnected"
            ? "Disconnected. Reconnecting..."
            : state === "error"
              ? error
              : state === "loading"
                ? "Loading logs..."
                : visible.length === 0
                  ? "No task logs yet."
                  : `${visible.length} records`}
        </p>
        <div className="mt-3 max-h-[460px] overflow-auto rounded-md bg-slate-950 p-3 font-mono text-[12px] text-slate-200">
          {visible.map((row) => (
            <div key={row.seq.toString()} className="border-b border-slate-800 py-2 last:border-0">
              <span className="text-teal-300">{row.event.phase}</span>{" "}
              <span className="text-teal-300">{row.event.action_id}</span>{" "}
              <span className="text-slate-500">
                {new Date(row.created / 1e6).toISOString()} {row.event.source}
                {row.event.phase !== "task" ? ` ${row.event.runtime} ${row.event.image}` : ""}
              </span>
              <div className="whitespace-pre-wrap text-slate-100">{row.event.message}</div>
            </div>
          ))}
        </div>
      </section>
    </main>
  );
}
