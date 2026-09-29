import { Button } from "@base-ui/react/button";
import { createClient } from "@connectrpc/connect";
import { useMutation } from "@connectrpc/connect-query";
import { createFileRoute } from "@tanstack/react-router";
import { useState, type FormEvent } from "react";
import { useEffect } from "react";
import { decode as decodeCbor, encode as encodeCbor } from "cborg";
import { downloadBlob } from "@/lib/blob";
import { decode } from "@/lib/value";
import { encodeHelloParameters } from "@/lib/hello";
import { transport } from "@/lib/rpc";
import { BlobService } from "@/proto/lutra/v1/blob_pb";
import { SettingsService } from "@/proto/lutra/v1/settings_pb";
import { runTask as runTaskMethod } from "@/proto/lutra/v1/lutra-LutraService_connectquery";

export const Route = createFileRoute("/")({ component: Home });
const blobRpc = createClient(BlobService, transport);
const settingsRpc = createClient(SettingsService, transport);

function Home() {
  const runTask = useMutation(runTaskMethod);
  const [parameters, setParameters] = useState('{"name":"world"}');
  const [result, setResult] = useState<{ value: unknown; contents?: string } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [projects, setProjects] = useState<
    Awaited<ReturnType<typeof settingsRpc.listProjects>>["projects"]
  >([]);
  const [domains, setDomains] = useState<
    Awaited<ReturnType<typeof settingsRpc.listDomains>>["domains"]
  >([]);
  const [projectId, setProjectId] = useState("");
  const [domainId, setDomainId] = useState("");
  const [settings, setSettings] = useState<
    Awaited<ReturnType<typeof settingsRpc.resolveSettings>>["settings"]
  >([]);
  const [settingPath, setSettingPath] = useState("");
  const [settingValue, setSettingValue] = useState("{}");
  const [settingsError, setSettingsError] = useState<string | null>(null);

  useEffect(() => {
    void settingsRpc
      .listProjects({})
      .then((response) => {
        setProjects(response.projects);
        if (response.projects[0]) setProjectId(response.projects[0].id);
      })
      .catch((cause) => setSettingsError(String(cause)));
  }, []);
  useEffect(() => {
    if (!projectId) {
      setDomains([]);
      return;
    }
    void settingsRpc
      .listDomains({ projectId })
      .then((response) => setDomains(response.domains))
      .catch((cause) => setSettingsError(String(cause)));
  }, [projectId]);
  useEffect(() => {
    if (!projectId) {
      setSettings([]);
      return;
    }
    void settingsRpc
      .resolveSettings({ projectId, domainId, paths: [] })
      .then((response) => setSettings(response.settings))
      .catch((cause) => setSettingsError(String(cause)));
  }, [projectId, domainId]);

  async function saveSetting(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSettingsError(null);
    try {
      const value = encodeCbor(JSON.parse(settingValue));
      await settingsRpc.upsertSetting({ projectId, domainId, path: settingPath, valueCbor: value });
      const response = await settingsRpc.resolveSettings({ projectId, domainId, paths: [] });
      setSettings(response.settings);
    } catch (cause) {
      setSettingsError(cause instanceof Error ? cause.message : String(cause));
    }
  }

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
      <section
        aria-labelledby="settings-title"
        className="mt-6 rounded-lg border border-slate-200 bg-white p-5"
      >
        <h2 id="settings-title" className="text-base font-semibold">
          Settings
        </h2>
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          <label className="text-[13px] font-semibold">
            Project
            <select
              value={projectId}
              onChange={(event) => {
                setProjectId(event.target.value);
                setDomainId("");
              }}
              className="mt-1 block w-full rounded-md border border-slate-300 p-2 font-normal"
            >
              {projects.map((project) => (
                <option key={project.id} value={project.id}>
                  {project.name} ({project.slug})
                </option>
              ))}
            </select>
          </label>
          <label className="text-[13px] font-semibold">
            Domain
            <select
              value={domainId}
              onChange={(event) => setDomainId(event.target.value)}
              className="mt-1 block w-full rounded-md border border-slate-300 p-2 font-normal"
            >
              <option value="">Project defaults</option>
              {domains.map((domain) => (
                <option key={domain.id} value={domain.id}>
                  {domain.name} ({domain.slug})
                </option>
              ))}
            </select>
          </label>
        </div>
        <form
          onSubmit={(event) => void saveSetting(event)}
          className="mt-4 grid gap-3 md:grid-cols-[1fr_1fr_auto] md:items-end"
        >
          <label className="text-[13px] font-semibold">
            Path
            <input
              required
              value={settingPath}
              onChange={(event) => setSettingPath(event.target.value)}
              placeholder="training/batch_size"
              className="mt-1 block w-full rounded-md border border-slate-300 p-2 font-mono text-[13px] font-normal"
            />
          </label>
          <label className="text-[13px] font-semibold">
            JSON value
            <input
              required
              value={settingValue}
              onChange={(event) => setSettingValue(event.target.value)}
              className="mt-1 block w-full rounded-md border border-slate-300 p-2 font-mono text-[13px] font-normal"
            />
          </label>
          <Button
            type="submit"
            disabled={!projectId}
            className="rounded-md bg-teal-700 px-4 py-2 text-[13px] font-semibold text-white hover:bg-teal-800 disabled:opacity-50"
          >
            Save
          </Button>
        </form>
        {settingsError && (
          <p role="alert" className="mt-3 text-[13px] text-rose-700">
            {settingsError}
          </p>
        )}
        <div className="mt-5 divide-y border-t border-slate-200">
          {settings.length === 0 && (
            <p className="py-4 text-[13px] text-slate-500">No settings yet.</p>
          )}
          {settings.map((setting) => (
            <div key={setting.path} className="grid gap-2 py-3 md:grid-cols-[1fr_2fr]">
              <span className="font-mono text-[13px] font-semibold">{setting.path}</span>
              <code className="break-all text-[13px] text-slate-600">
                {JSON.stringify(decodeCbor(setting.valueCbor))}
              </code>
            </div>
          ))}
        </div>
      </section>
    </main>
  );
}
