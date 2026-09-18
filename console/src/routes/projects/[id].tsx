import { Title } from "@solidjs/meta";
import { useParams } from "@solidjs/router";
import { For, Loading, Show, createMemo, refresh } from "solid-js";
import {
  EmptyState,
  QueryBoundary,
  RunStatusBadge,
  SkeletonRows,
} from "../../components/StatusBadge";
import { dateTimeLabel } from "../../lib/format";
import { fetchProjectDashboard, type ProjectDashboard } from "../../lib/data";

function ProjectContent(props: { data: () => ProjectDashboard; onRetry: () => void }) {
  return (
    <QueryBoundary message="Unable to load project." onRetry={props.onRetry}>
      <div class="mt-5 flex flex-col justify-between gap-5 border-b border-slate-200 pb-8 dark:border-slate-800 sm:flex-row sm:items-end">
        <div>
          <p class="text-xs font-bold uppercase tracking-[0.18em] text-cyan-700 dark:text-cyan-300">
            Project
          </p>
          <h1 class="mt-2 text-3xl font-black tracking-tight text-slate-950 dark:text-white">
            {props.data().project.name}
          </h1>
          <p class="mono mt-2 text-xs text-slate-500 dark:text-slate-400">
            {props.data().project.projectId}
          </p>
        </div>
        <div class="flex gap-2 text-xs text-slate-500 dark:text-slate-400">
          <span class="rounded-full bg-slate-100 px-3 py-1.5 dark:bg-slate-800">
            {props.data().tasks.length} tasks
          </span>
          <span class="rounded-full bg-slate-100 px-3 py-1.5 dark:bg-slate-800">
            {props.data().runs.length} runs
          </span>
        </div>
      </div>

      <section class="mt-8">
        <div class="mb-4 flex items-end justify-between">
          <div>
            <h2 class="text-lg font-bold text-slate-950 dark:text-white">Registered tasks</h2>
            <p class="mt-1 text-sm text-slate-500 dark:text-slate-400">
              Versions available to this project.
            </p>
          </div>
        </div>
        <Show
          when={props.data().tasks.length > 0}
          fallback={
            <EmptyState
              title="No tasks registered"
              detail="Register a task through the SDK to see it here."
            />
          }
        >
          <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            <For each={props.data().tasks}>
              {(task) => (
                <article class="rounded-xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900">
                  <div class="flex items-start justify-between gap-3">
                    <span
                      class="truncate rounded-full bg-cyan-50 px-2.5 py-1 text-[10px] font-bold text-cyan-700 dark:bg-cyan-950/60 dark:text-cyan-300"
                      title={task.version}
                    >
                      {task.version}
                    </span>
                    <span class="text-xs text-slate-400">
                      {task.inputSlots.length} in · {task.outputSlots.length} out
                    </span>
                  </div>
                  <h3 class="mt-4 truncate font-bold text-slate-950 dark:text-white">
                    {task.name}
                  </h3>
                  <p class="mono mt-1 truncate text-xs text-slate-500 dark:text-slate-400">
                    {task.taskId}
                  </p>
                </article>
              )}
            </For>
          </div>
        </Show>
      </section>

      <section class="mt-12">
        <div class="mb-4 flex items-end justify-between">
          <div>
            <h2 class="text-lg font-bold text-slate-950 dark:text-white">Recent runs</h2>
            <p class="mt-1 text-sm text-slate-500 dark:text-slate-400">
              Execution history for this project.
            </p>
          </div>
          <span class="text-xs font-semibold text-slate-400">{props.data().runs.length} total</span>
        </div>
        <Show
          when={props.data().runs.length > 0}
          fallback={
            <EmptyState title="No runs yet" detail="Runs created from the SDK will appear here." />
          }
        >
          <div class="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-900">
            <div class="divide-y divide-slate-200 dark:divide-slate-800">
              <For each={props.data().runs}>
                {(run) => (
                  <a
                    href={`/projects/${props.data().project.projectId}/runs/${run.runId}`}
                    class="flex flex-col gap-3 px-5 py-4 transition hover:bg-slate-50 sm:flex-row sm:items-center sm:justify-between dark:hover:bg-slate-800/60"
                  >
                    <div class="min-w-0">
                      <div class="mono truncate text-xs font-semibold text-slate-700 dark:text-slate-300">
                        {run.runId}
                      </div>
                      <div class="mt-1 text-xs text-slate-500 dark:text-slate-400">
                        Created {dateTimeLabel(run.createTime)}
                      </div>
                    </div>
                    <RunStatusBadge state={run.state} />
                  </a>
                )}
              </For>
            </div>
          </div>
        </Show>
      </section>
    </QueryBoundary>
  );
}

export default function ProjectPage() {
  const params = useParams<{ id: string }>();
  const data = createMemo(() => fetchProjectDashboard(params.id));

  return (
    <main class="mx-auto max-w-7xl px-5 py-8 lg:px-10 lg:py-12">
      <Title>Project · Lutra</Title>
      <a href="/" class="text-xs font-bold text-cyan-700 hover:underline dark:text-cyan-300">
        ← All projects
      </a>
      <section class="mt-8">
        <Loading fallback={<SkeletonRows count={5} />}>
          <ProjectContent data={data} onRetry={() => void refresh(data)} />
        </Loading>
      </section>
    </main>
  );
}
