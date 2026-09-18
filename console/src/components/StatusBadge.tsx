import { Errored, For, Show } from "solid-js";
import type { JSX } from "@solidjs/web";
import { ActionState, RunState } from "../proto/lutra/v1/run_pb";

export function errorMessage(value: unknown, fallback: string): string {
  return value instanceof Error ? value.message : fallback;
}

// Tone is the shared severity classification for action and run states; the
// badge and the timeline dot both render from it so they cannot drift.
type Tone = "success" | "danger" | "warning" | "active" | "waiting" | "idle";

interface StateInfo {
  label: string;
  tone: Tone;
}

const badgeTones: Record<Tone, string> = {
  success:
    "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/50 dark:text-emerald-300",
  danger:
    "border-red-200 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950/50 dark:text-red-300",
  warning:
    "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950/50 dark:text-amber-300",
  active:
    "border-cyan-200 bg-cyan-50 text-cyan-700 dark:border-cyan-900 dark:bg-cyan-950/50 dark:text-cyan-300",
  waiting:
    "border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-900 dark:bg-violet-950/50 dark:text-violet-300",
  idle: "border-slate-200 bg-slate-100 text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300",
};

const dotTones: Record<Tone, string> = {
  success: "bg-emerald-500",
  danger: "bg-red-500",
  warning: "bg-amber-500",
  active: "animate-pulse bg-cyan-500",
  waiting: "animate-pulse bg-violet-500",
  idle: "bg-slate-400",
};

export function dotClass(tone: Tone): string {
  return dotTones[tone];
}

const unknownState: StateInfo = { label: "Unknown", tone: "idle" };

const actionStateInfos: Partial<Record<ActionState, StateInfo>> = {
  [ActionState.READY]: { label: "Ready", tone: "idle" },
  [ActionState.RUNNING]: { label: "Running", tone: "active" },
  [ActionState.SUCCEEDED]: { label: "Succeeded", tone: "success" },
  [ActionState.FAILED]: { label: "Failed", tone: "danger" },
  [ActionState.CANCELED]: { label: "Canceled", tone: "warning" },
  [ActionState.TIMED_OUT]: { label: "Timed out", tone: "danger" },
};

const runStateInfos: Partial<Record<RunState, StateInfo>> = {
  [RunState.QUEUED]: { label: "Queued", tone: "idle" },
  [RunState.RUNNING]: { label: "Running", tone: "active" },
  [RunState.SUCCEEDED]: { label: "Succeeded", tone: "success" },
  [RunState.FAILED]: { label: "Failed", tone: "danger" },
  [RunState.CANCELLED]: { label: "Canceled", tone: "warning" },
};

export function actionStateInfo(state: ActionState | undefined): StateInfo {
  return (state !== undefined && actionStateInfos[state]) || unknownState;
}

export function runStateInfo(state: RunState | undefined): StateInfo {
  return (state !== undefined && runStateInfos[state]) || unknownState;
}

function Badge(props: { info: StateInfo; label?: string; compact?: boolean }) {
  return (
    <span
      class={[
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-bold",
        props.compact ? "px-2 py-0.5 text-[10px]" : "",
        badgeTones[props.info.tone],
      ]}
    >
      <span class={["h-1.5 w-1.5 rounded-full", dotTones[props.info.tone]]} />
      {props.label ?? props.info.label}
    </span>
  );
}

export function StatusBadge(props: { state?: ActionState; label?: string; compact?: boolean }) {
  return <Badge info={actionStateInfo(props.state)} label={props.label} compact={props.compact} />;
}

export function RunStatusBadge(props: { state?: RunState }) {
  return <Badge info={runStateInfo(props.state)} />;
}

// QueryBoundary owns the error panel and retry wiring shared by every
// data-loading page.
export function QueryBoundary(props: {
  message: string;
  onRetry: () => void;
  children: JSX.Element;
}) {
  return (
    <Errored
      fallback={(error, reset) => (
        <ErrorPanel
          message={errorMessage(error(), props.message)}
          onRetry={() => {
            reset();
            props.onRetry();
          }}
        />
      )}
    >
      {props.children}
    </Errored>
  );
}

export function ErrorPanel(props: { message: string; onRetry?: () => void }) {
  return (
    <div class="rounded-xl border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/50 dark:text-red-300">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <p>{props.message}</p>
        <Show when={props.onRetry}>
          <button
            class="font-bold underline underline-offset-4"
            type="button"
            onClick={() => props.onRetry?.()}
          >
            Try again
          </button>
        </Show>
      </div>
    </div>
  );
}

export function EmptyState(props: { title: string; detail: string }) {
  return (
    <div class="rounded-2xl border border-dashed border-slate-300 bg-white px-6 py-14 text-center dark:border-slate-700 dark:bg-slate-900">
      <div class="mx-auto grid h-11 w-11 place-items-center rounded-xl bg-cyan-50 text-lg text-cyan-700 dark:bg-cyan-950/50 dark:text-cyan-300">
        ○
      </div>
      <h3 class="mt-4 text-sm font-bold text-slate-950 dark:text-white">{props.title}</h3>
      <p class="mx-auto mt-1 max-w-sm text-sm text-slate-500 dark:text-slate-400">{props.detail}</p>
    </div>
  );
}

export function SkeletonRows(props: { count?: number }) {
  return (
    <div class="space-y-2" aria-label="Loading">
      <For each={Array.from({ length: props.count ?? 3 })}>
        {() => <div class="h-16 animate-pulse rounded-xl bg-slate-200 dark:bg-slate-800" />}
      </For>
    </div>
  );
}
