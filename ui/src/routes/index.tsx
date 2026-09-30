import { Button } from "@base-ui/react/button";
import { createClient } from "@connectrpc/connect";
import { createFileRoute } from "@tanstack/react-router";
import { useState, type FormEvent } from "react";
import { useEffect } from "react";
import { decode as decodeCbor, encode as encodeCbor } from "cborg";
import { transport } from "@/lib/rpc";
import { SettingsService } from "@/proto/lutra/v1/settings_pb";
import { Link } from "@tanstack/react-router";

export const Route = createFileRoute("/")({ component: Home });
const settingsRpc = createClient(SettingsService, transport);

function Home() {
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
  const [runId, setRunId] = useState("");

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

  return (
    <main className="mx-auto max-w-[1050px] px-5 py-8 md:px-10 md:py-11">
      <div className="mb-10">
        <p className="mb-2 text-[11px] font-bold tracking-widest text-teal-700 uppercase">
          Workspace
        </p>
        <h1 className="text-[29px] leading-tight font-bold">Settings</h1>
      </div>
      <section
        aria-labelledby="settings-title"
        className="rounded-lg border border-slate-200 bg-white p-5"
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
      <section className="mt-5 rounded-lg border border-slate-200 bg-white p-5">
        <h2 className="text-base font-semibold">Open a run</h2>
        <form className="mt-3 flex flex-wrap gap-2" onSubmit={(event) => event.preventDefault()}>
          <input
            aria-label="Run ID"
            value={runId}
            onChange={(event) => setRunId(event.target.value)}
            placeholder="Run ID"
            className="min-w-0 flex-1 rounded-md border border-slate-300 p-2 font-mono text-[13px]"
          />
          <Link
            to="/runs/$runId"
            params={{ runId }}
            className="rounded-md bg-teal-700 px-4 py-2 text-[13px] font-semibold text-white hover:bg-teal-800"
          >
            Open
          </Link>
        </form>
      </section>
    </main>
  );
}
