import { Button } from "@base-ui/react/button";
import { createClient } from "@connectrpc/connect";
import { useMutation } from "@connectrpc/connect-query";
import { createFileRoute } from "@tanstack/react-router";
import { useState, type FormEvent } from "react";
import { downloadBlob } from "@/lib/blob";
import { decode } from "@/lib/value";
import { encodeHelloParameters } from "@/lib/hello";
import { transport } from "@/lib/rpc";
import { BlobService } from "@/proto/lutra/v1/blob_pb";
import { runTask as runTaskMethod } from "@/proto/lutra/v1/lutra-LutraService_connectquery";

export const Route = createFileRoute("/")({ component: Home });
const blobRpc = createClient(BlobService, transport);

function Home() {
  const runTask = useMutation(runTaskMethod);
  const [parameters, setParameters] = useState('{"name":"world"}');
  const [result, setResult] = useState<{ value: unknown; contents?: string } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setResult(null);
    setBusy(true);
    try {
      const parametersCbor = encodeHelloParameters(parameters);
      const response = await runTask.mutateAsync({ taskName: "hello", parametersCbor });
      const value = await decode(response.resultCbor, async (uri) => downloadBlob(blobRpc, uri));
      setResult({ value });
      if (typeof value === "string") setResult({ value, contents: value });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="mx-auto max-w-[1050px] px-5 py-8 md:px-10 md:py-11">
      <div className="mb-10">
        <p className="mb-2 text-[11px] font-bold tracking-widest text-teal-700 uppercase">
          Workspace
        </p>
        <h1 className="text-[29px] leading-tight font-bold">Overview</h1>
        <p className="mt-2 text-[13px] text-slate-500">
          Run a Python task and view its blob result.
        </p>
      </div>
      <section
        aria-labelledby="hello-title"
        className="rounded-lg border border-slate-200 bg-white p-5"
      >
        <h2 id="hello-title" className="text-base font-semibold">
          Hello task
        </h2>
        <p className="mt-1 text-[13px] text-slate-500">
          Send JSON parameters as CBOR to Python and store the greeting as a blob.
        </p>
        <form onSubmit={(event) => void submit(event)} className="mt-5 space-y-3">
          <label htmlFor="hello-parameters" className="block text-[13px] font-semibold">
            JSON parameters
          </label>
          <textarea
            id="hello-parameters"
            value={parameters}
            onChange={(event) => setParameters(event.target.value)}
            rows={4}
            spellCheck={false}
            className="w-full rounded-md border border-slate-300 p-3 font-mono text-[13px] focus-visible:outline-2 focus-visible:outline-teal-600"
          />
          <Button
            type="submit"
            disabled={busy}
            className="rounded-md bg-teal-700 px-4 py-2 text-[13px] font-semibold text-white hover:bg-teal-800 disabled:cursor-wait disabled:opacity-50"
          >
            {busy ? "Running..." : "Run hello"}
          </Button>
        </form>
        {error && (
          <p role="alert" className="mt-4 text-[13px] text-rose-700">
            {error}
          </p>
        )}
        {result && (
          <div className="mt-5 border-t border-slate-200 pt-4 text-[13px]">
            <p className="font-semibold">Task result</p>
            <p className="mt-2 break-all font-mono text-slate-600">
              {JSON.stringify(result.value)}
            </p>
            {result.contents !== undefined && (
              <pre className="mt-3 whitespace-pre-wrap rounded-md bg-slate-50 p-3 font-mono">
                {result.contents}
              </pre>
            )}
          </div>
        )}
      </section>
    </main>
  );
}
