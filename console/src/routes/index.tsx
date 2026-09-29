import { Button } from "@base-ui/react/button";
import { useQuery } from "@connectrpc/connect-query";
import { createFileRoute } from "@tanstack/react-router";
import { RefreshCw } from "lucide-react";
import { ping as pingMethod } from "@/proto/lutra/v1/lutra-LutraService_connectquery";

export const Route = createFileRoute("/")({ component: Home });

function Home() {
  const ping = useQuery(pingMethod, { message: "ping" }, { retry: false });

  return (
    <main className="mx-auto max-w-[1050px] px-5 py-8 md:px-10 md:py-11">
      <div className="mb-10">
        <p className="mb-2 text-[11px] font-bold tracking-widest text-teal-700 uppercase">
          Workspace
        </p>
        <h1 className="text-[29px] leading-tight font-bold">Overview</h1>
        <p className="mt-2 text-[13px] text-slate-500">Connection to the Lutra API.</p>
      </div>
      <section aria-labelledby="connection-title">
        <div className="flex items-center justify-between gap-4 border-b border-slate-200 pb-3">
          <div>
            <h2 id="connection-title" className="text-base font-semibold">
              Connection
            </h2>
            <p className="mt-1 text-xs text-slate-500">Current API response</p>
          </div>
          <Button
            className="grid size-8 place-items-center rounded-md border border-slate-200 bg-white text-slate-600 hover:bg-teal-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-teal-600 disabled:cursor-wait disabled:opacity-50"
            onClick={() => void ping.refetch()}
            disabled={ping.isFetching}
            title="Refresh connection"
            aria-label="Refresh connection"
          >
            <RefreshCw size={18} className={ping.isFetching ? "animate-spin" : ""} />
          </Button>
        </div>
        <div
          className="flex min-h-16 flex-wrap items-center gap-3 border-b border-slate-200 bg-white px-4 py-3 text-[13px]"
          role="status"
        >
          <span
            className={`size-2 shrink-0 rounded-full ${ping.isError ? "bg-rose-500" : ping.isPending ? "bg-amber-400" : "bg-emerald-500"}`}
          />
          <span className="font-semibold">RPC service</span>
          <span className="min-w-0 [overflow-wrap:anywhere] text-slate-500 sm:ml-auto sm:text-right">
            {ping.isPending
              ? "Connecting..."
              : ping.isError
                ? ping.error.message
                : ping.data.message}
          </span>
        </div>
      </section>
    </main>
  );
}
