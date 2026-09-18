import { Title } from "@solidjs/meta";
import { useParams } from "@solidjs/router";
import {
  For,
  Loading,
  Show,
  createEffect,
  createMemo,
  createSignal,
  onCleanup,
  refresh,
} from "solid-js";
import {
  EmptyState,
  ErrorPanel,
  QueryBoundary,
  RunStatusBadge,
  SkeletonRows,
  StatusBadge,
  actionStateInfo,
  dotClass,
  errorMessage,
} from "../../../../components/StatusBadge";
import { cancelRun, fetchRunDetails, type RunDetails } from "../../../../lib/data";
import { dateTimeLabel, durationLabel, timestampMs } from "../../../../lib/format";
import { RunState, type Action } from "../../../../proto/lutra/v1/run_pb";

function actionTimestamp(action: Action): number {
  return timestampMs(action.createTime) ?? Number.POSITIVE_INFINITY;
}

type TimelineNode = { action: Action; children: TimelineNode[] };

function buildTimeline(actions: Action[]): TimelineNode[] {
  const nodes = new Map<string, TimelineNode>();
  for (const action of actions) nodes.set(action.actionId, { action, children: [] });

  const roots: TimelineNode[] = [];
  for (const node of nodes.values()) {
    const parent = node.action.parentActionId ? nodes.get(node.action.parentActionId) : undefined;
    if (parent) parent.children.push(node);
    else roots.push(node);
  }

  const sortNodes = (items: TimelineNode[]) => {
    items.sort((left, right) => actionTimestamp(left.action) - actionTimestamp(right.action));
    for (const item of items) sortNodes(item.children);
  };
  sortNodes(roots);
  return roots;
}

function actionName(action: Action): string {
  return action.taskId;
}

// ActionStatus.duration_ms is a plain proto uint64, so it is 0n rather than
// undefined until the action finishes. Key the label off end_time instead, so
// a running action reads "in progress" and not "0 ms".
function actionDuration(action: Action): bigint | undefined {
  const status = action.status;
  return status?.endTime === undefined ? undefined : status.durationMs;
}

function isTerminal(state: RunState): boolean {
  return [RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED].includes(state);
}

function bindingLabel(binding: Action["inputs"][number]): string {
  if (binding.value.case === "artifactId") return `artifact ${binding.value.value}`;
  if (binding.value.case === "inlineBytes") return `${binding.value.value.length} inline bytes`;
  return "empty";
}

function TimelineNodeView(props: { node: TimelineNode; depth: number }) {
  const action = () => props.node.action;
  return (
    <article class="relative pl-8" style={{ "margin-left": `${props.depth * 1.5}rem` }}>
      <span
        class={[
          "absolute left-0 top-5 z-10 h-6 w-6 rounded-full border-4 border-slate-50 dark:border-slate-950",
          dotClass(actionStateInfo(action().status?.state).tone),
        ]}
      />
      <details class="group overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-900">
        <summary class="cursor-pointer list-none p-5 select-none [&::-webkit-details-marker]:hidden">
          <div class="flex flex-wrap items-start justify-between gap-3">
            <div class="min-w-0">
              <p class="mono truncate text-[10px] text-slate-400">{action().actionId}</p>
              <h3 class="mt-1 truncate font-bold text-slate-950 dark:text-white">
                {actionName(action())}
              </h3>
            </div>
            <StatusBadge state={action().status?.state} compact />
          </div>
          <div class="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-500 dark:text-slate-400">
            <span>Attempt {action().status?.attemptCount ?? 0}</span>
            <span>{durationLabel(actionDuration(action()))}</span>
            <Show when={action().parentActionId}>
              <span>Child action</span>
            </Show>
          </div>
          <Show when={action().status?.failureMessage}>
            <p class="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-xs text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300">
              {action().status?.failureMessage}
            </p>
          </Show>
        </summary>
        <div class="border-t border-slate-200 px-5 pb-5 pt-4 dark:border-slate-800">
          <dl class="grid gap-4 text-sm sm:grid-cols-2">
            <div>
              <dt class="text-[10px] font-bold uppercase tracking-wide text-slate-400">Created</dt>
              <dd class="mt-1 text-slate-700 dark:text-slate-300">
                {dateTimeLabel(action().createTime)}
              </dd>
            </div>
            <div>
              <dt class="text-[10px] font-bold uppercase tracking-wide text-slate-400">Updated</dt>
              <dd class="mt-1 text-slate-700 dark:text-slate-300">
                {dateTimeLabel(action().updateTime)}
              </dd>
            </div>
            <div>
              <dt class="text-[10px] font-bold uppercase tracking-wide text-slate-400">Started</dt>
              <dd class="mt-1 text-slate-700 dark:text-slate-300">
                {dateTimeLabel(action().status?.startTime)}
              </dd>
            </div>
            <div>
              <dt class="text-[10px] font-bold uppercase tracking-wide text-slate-400">Finished</dt>
              <dd class="mt-1 text-slate-700 dark:text-slate-300">
                {dateTimeLabel(action().status?.endTime)}
              </dd>
            </div>
          </dl>
          <div class="mt-5 grid gap-5 border-t border-slate-200 pt-4 sm:grid-cols-2 dark:border-slate-800">
            <BindingList title="Inputs" bindings={action().inputs} />
            <BindingList title="Outputs" bindings={action().outputs} />
          </div>
        </div>
      </details>
      <Show when={props.node.children.length > 0}>
        <div class="relative ml-4 mt-4 space-y-4 border-l border-slate-200 pl-4 dark:border-slate-800">
          <For each={props.node.children}>
            {(child) => <TimelineNodeView node={child} depth={props.depth + 1} />}
          </For>
        </div>
      </Show>
    </article>
  );
}

function BindingList(props: { title: string; bindings: Action["inputs"] }) {
  return (
    <div>
      <h4 class="text-[10px] font-bold uppercase tracking-wide text-slate-400">{props.title}</h4>
      <Show
        when={props.bindings.length > 0}
        fallback={<p class="mt-2 text-xs text-slate-400">None</p>}
      >
        <ul class="mt-2 space-y-1 text-xs text-slate-600 dark:text-slate-300">
          <For each={props.bindings}>
            {(binding) => (
              <li>
                <span class="text-slate-400">{binding.slotName}:</span> {bindingLabel(binding)}
              </li>
            )}
          </For>
        </ul>
      </Show>
    </div>
  );
}

function RunContent(props: {
  data: () => RunDetails;
  onRetry: () => void;
  onCancel: () => Promise<void>;
  canceling: boolean;
  cancelError: string;
}) {
  const timeline = createMemo(() => buildTimeline(props.data().actions));
  const run = () => props.data().run;

  return (
    <QueryBoundary message="Unable to load run." onRetry={props.onRetry}>
      <div class="mt-5 flex flex-col justify-between gap-5 border-b border-slate-200 pb-8 dark:border-slate-800 sm:flex-row sm:items-end">
        <div class="min-w-0">
          <p class="text-xs font-bold uppercase tracking-[0.18em] text-cyan-700 dark:text-cyan-300">
            Run details
          </p>
          <h1 class="mt-2 truncate text-2xl font-black tracking-tight text-slate-950 dark:text-white sm:text-3xl">
            {run().runId}
          </h1>
          <p class="mt-2 text-xs text-slate-500 dark:text-slate-400">
            Created {dateTimeLabel(run().createTime)}
          </p>
        </div>
        <div class="flex flex-wrap items-center gap-3">
          <RunStatusBadge state={run().state} />
          <Show when={[RunState.QUEUED, RunState.RUNNING].includes(run().state)}>
            <button
              disabled={props.canceling}
              class="rounded-lg border border-red-200 px-3 py-2 text-xs font-bold text-red-700 transition hover:bg-red-50 disabled:cursor-wait disabled:opacity-50 dark:border-red-900 dark:text-red-300 dark:hover:bg-red-950/40"
              onClick={() => void props.onCancel()}
            >
              {props.canceling ? "Canceling…" : "Cancel run"}
            </button>
          </Show>
        </div>
      </div>

      <Show when={props.cancelError}>
        <div class="mt-5">
          <ErrorPanel message={props.cancelError} onRetry={() => void props.onCancel()} />
        </div>
      </Show>

      <section class="mt-8">
        <div class="mb-5 flex items-end justify-between">
          <div>
            <h2 class="text-lg font-bold text-slate-950 dark:text-white">Execution timeline</h2>
            <p class="mt-1 text-sm text-slate-500 dark:text-slate-400">
              Expand an action to inspect attempts, bindings, and timing.
            </p>
          </div>
          <span class="text-xs font-semibold text-slate-400">
            {props.data().actions.length} actions
          </span>
        </div>
        <Show
          when={props.data().actions.length > 0}
          fallback={
            <EmptyState
              title="No actions yet"
              detail="Actions will appear as the run is scheduled."
            />
          }
        >
          <div class="relative">
            <div class="pointer-events-none absolute bottom-6 left-3 top-6 w-px bg-slate-200 dark:bg-slate-800" />
            <div class="space-y-4">
              <For each={timeline()}>{(node) => <TimelineNodeView node={node} depth={0} />}</For>
            </div>
          </div>
        </Show>
      </section>
    </QueryBoundary>
  );
}

export default function RunPage() {
  const params = useParams<{ id: string; runId: string }>();
  const [canceling, setCanceling] = createSignal(false);
  const [cancelError, setCancelError] = createSignal("");
  const data = createMemo(() => fetchRunDetails(params.id, params.runId));

  createEffect(
    () => params.runId,
    () => {
      let timer: number | undefined;
      let disposed = false;
      const poll = async () => {
        try {
          const current = await refresh(data);
          if (disposed || isTerminal(current.run.state)) return;
        } catch {
          if (disposed) return;
        }
        if (!disposed) timer = window.setTimeout(() => void poll(), 1_500);
      };

      timer = window.setTimeout(() => void poll(), 1_500);
      onCleanup(() => {
        disposed = true;
        if (timer !== undefined) window.clearTimeout(timer);
      });
    },
  );

  const cancel = async () => {
    setCanceling(true);
    setCancelError("");
    try {
      await cancelRun(params.id, params.runId);
      await refresh(data);
    } catch (error) {
      // The click handler discards this promise, so a failed cancel has to
      // surface here or it is lost as an unhandled rejection.
      setCancelError(errorMessage(error, "Unable to cancel this run."));
    } finally {
      setCanceling(false);
    }
  };

  return (
    <main class="mx-auto max-w-7xl px-5 py-8 lg:px-10 lg:py-12">
      <Title>Run {params.runId} · Lutra</Title>
      <a
        href={`/projects/${params.id}`}
        class="text-xs font-bold text-cyan-700 hover:underline dark:text-cyan-300"
      >
        ← Back to project
      </a>
      <section class="mt-8">
        <Loading fallback={<SkeletonRows count={4} />}>
          <RunContent
            data={data}
            onRetry={() => void refresh(data)}
            onCancel={cancel}
            canceling={canceling()}
            cancelError={cancelError()}
          />
        </Loading>
      </section>
    </main>
  );
}
