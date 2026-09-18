import { Title } from "@solidjs/meta";
import { For, Loading, Show, createMemo, refresh } from "solid-js";
import { EmptyState, QueryBoundary, SkeletonRows } from "../components/StatusBadge";
import { dateLabel } from "../lib/format";
import { fetchProjects } from "../lib/data";
import { apiKey, saveAPIKey } from "../lib/session";
import type { Project } from "../proto/lutra/v1/project_pb";

function ConnectCard() {
  const connect = (event: SubmitEvent) => {
    event.preventDefault();
    const input = (event.currentTarget as HTMLFormElement).elements.namedItem(
      "api-key",
    ) as HTMLInputElement;
    saveAPIKey(input.value);
    input.value = "";
  };

  return (
    <section class="mt-10 max-w-xl rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-800 dark:bg-slate-900 sm:p-8">
      <div class="grid h-11 w-11 place-items-center rounded-xl bg-cyan-50 text-lg text-cyan-700 dark:bg-cyan-950/50 dark:text-cyan-300">
        ↗
      </div>
      <h2 class="mt-5 text-xl font-bold text-slate-950 dark:text-white">
        Connect your control plane
      </h2>
      <p class="mt-2 text-sm leading-6 text-slate-500 dark:text-slate-400">
        Add an API key to load projects, runs, and task execution details from your Lutra server.
      </p>
      <form class="mt-6" onSubmit={connect}>
        <label
          class="text-xs font-bold uppercase tracking-wide text-slate-500 dark:text-slate-400"
          for="api-key"
        >
          API key
        </label>
        <div class="mt-2 flex flex-col gap-2 sm:flex-row">
          <input
            id="api-key"
            class="min-w-0 flex-1 rounded-lg border border-slate-300 bg-white px-3 py-2.5 text-sm text-slate-950 outline-none placeholder:text-slate-400 focus:border-cyan-600 dark:border-slate-700 dark:bg-slate-950 dark:text-white dark:focus:border-cyan-300"
            type="password"
            autocomplete="off"
            placeholder="lutra_…"
            required
          />
          <button
            class="rounded-lg bg-cyan-700 px-4 py-2.5 text-sm font-bold text-white transition hover:bg-cyan-800 dark:bg-cyan-200 dark:text-slate-950 dark:hover:bg-cyan-100"
            type="submit"
          >
            Connect
          </button>
        </div>
      </form>
    </section>
  );
}

function ProjectList(props: { projects: () => Project[]; onRetry: () => void }) {
  return (
    <QueryBoundary message="Unable to load projects." onRetry={props.onRetry}>
      <Show
        when={props.projects().length > 0}
        fallback={
          <EmptyState
            title="No projects yet"
            detail="Projects registered with your API key will appear here."
          />
        }
      >
        <div class="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-900">
          <div class="hidden border-b border-slate-200 bg-slate-50 px-5 py-3 text-[10px] font-bold uppercase tracking-[0.16em] text-slate-500 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-500 sm:grid sm:grid-cols-[1fr_1.2fr_120px] sm:gap-4">
            <span>Project</span>
            <span>Identifier</span>
            <span>Updated</span>
          </div>
          <div class="divide-y divide-slate-200 dark:divide-slate-800">
            <For each={props.projects()}>
              {(project) => (
                <a
                  href={`/projects/${project.projectId}`}
                  class="grid gap-2 px-5 py-4 transition hover:bg-slate-50 sm:grid-cols-[1fr_1.2fr_120px] sm:items-center sm:gap-4 dark:hover:bg-slate-800/60"
                >
                  <div class="flex items-center gap-3">
                    <span class="grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-cyan-50 text-sm font-black text-cyan-700 dark:bg-cyan-950/60 dark:text-cyan-300">
                      {project.name.slice(0, 1).toUpperCase()}
                    </span>
                    <div class="min-w-0">
                      <div class="truncate font-bold text-slate-950 dark:text-white">
                        {project.name}
                      </div>
                      <div class="text-xs text-slate-500 sm:hidden">Project</div>
                    </div>
                  </div>
                  <span class="mono truncate text-xs text-slate-500 dark:text-slate-400">
                    {project.projectId}
                  </span>
                  <span class="text-xs text-slate-500 dark:text-slate-400">
                    {dateLabel(project.updateTime ?? project.createTime)}
                  </span>
                </a>
              )}
            </For>
          </div>
        </div>
      </Show>
    </QueryBoundary>
  );
}

export default function Home() {
  const projects = createMemo(() => {
    if (!apiKey()) return Promise.resolve([] as Project[]);
    return fetchProjects();
  });

  return (
    <main class="mx-auto max-w-7xl px-5 py-8 lg:px-10 lg:py-12">
      <Title>Projects · Lutra</Title>
      <div class="flex flex-col justify-between gap-6 md:flex-row md:items-end">
        <div>
          <p class="text-xs font-bold uppercase tracking-[0.18em] text-cyan-700 dark:text-cyan-300">
            Workspace
          </p>
          <h1 class="mt-3 text-3xl font-black tracking-tight text-slate-950 dark:text-white sm:text-4xl">
            Projects
          </h1>
          <p class="mt-2 max-w-xl text-sm text-slate-500 dark:text-slate-400">
            Monitor registered workflows and inspect every execution from one place.
          </p>
        </div>
      </div>

      <Show when={apiKey()} fallback={<ConnectCard />}>
        <section class="mt-10">
          <Loading fallback={<SkeletonRows count={4} />}>
            <ProjectList projects={projects} onRetry={() => void refresh(projects)} />
          </Loading>
        </section>
      </Show>
    </main>
  );
}
