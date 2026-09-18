import { Title } from "@solidjs/meta";
import { useLocation } from "@solidjs/router";
import { Errored, Show } from "solid-js";
import { Router } from "./router";
import { clearAPIKey, apiKey } from "./lib/session";
import { ErrorPanel, errorMessage } from "./components/StatusBadge";
import "./App.css";

function NavItem(props: { href: string; label: string; icon: string; active: boolean }) {
  return (
    <a
      href={props.href}
      aria-current={props.active ? "page" : undefined}
      class={[
        "flex items-center gap-3 rounded-lg border-l-2 px-3 py-2.5 text-sm font-semibold transition",
        props.active
          ? "border-cyan-600 bg-slate-100 text-slate-950 dark:border-cyan-300 dark:bg-slate-900 dark:text-white"
          : "border-transparent text-slate-500 hover:bg-slate-100 hover:text-slate-950 dark:text-slate-400 dark:hover:bg-slate-900 dark:hover:text-white",
      ]}
    >
      <span class="grid h-7 w-7 place-items-center rounded-md bg-slate-200 text-xs font-bold text-cyan-700 dark:bg-slate-800 dark:text-cyan-300">
        {props.icon}
      </span>
      {props.label}
    </a>
  );
}

function AppShell(props: { children: unknown }) {
  const location = useLocation();
  const connected = () => Boolean(apiKey());
  const onDisconnect = () => {
    clearAPIKey();
    window.location.assign("/");
  };

  return (
    <div class="flex min-h-screen flex-col bg-slate-50 text-slate-950 dark:bg-slate-950 dark:text-slate-100 lg:flex-row">
      <aside class="flex w-full shrink-0 flex-col border-b border-slate-200 bg-white px-4 py-4 dark:border-slate-800 dark:bg-slate-950 lg:sticky lg:top-0 lg:h-screen lg:w-64 lg:border-b-0 lg:border-r lg:px-5 lg:py-6">
        <div class="flex items-center justify-between lg:block">
          <a
            href="/"
            class="inline-flex items-center gap-2.5 text-lg font-black tracking-tight text-slate-950 dark:text-white"
          >
            <span class="grid h-9 w-9 place-items-center rounded-xl bg-cyan-800 text-sm text-white shadow-sm dark:bg-cyan-200 dark:text-slate-950">
              L
            </span>
            lutra<span class="text-cyan-600 dark:text-cyan-300">/</span>
          </a>
          <span class="rounded-full border border-slate-200 bg-slate-100 px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.16em] text-slate-400 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-500">
            Console
          </span>
        </div>
        <div class="mt-8 hidden text-[10px] font-bold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-600 lg:block">
          Workspace
        </div>
        <nav class="mt-3 flex gap-2 overflow-x-auto lg:block lg:space-y-1">
          <NavItem
            href="/"
            label="Projects"
            icon="⌂"
            active={location.pathname === "/" || location.pathname.startsWith("/projects/")}
          />
        </nav>
        <div class="mt-auto hidden border-t border-slate-200 pt-5 dark:border-slate-800 lg:block">
          <div class="flex items-center gap-2 text-xs font-semibold text-slate-500 dark:text-slate-400">
            <span
              class={["h-2 w-2 rounded-full", connected() ? "bg-emerald-500" : "bg-amber-400"]}
            />
            {connected() ? "Connected to control plane" : "Not connected"}
          </div>
          <Show when={connected()}>
            <button
              class="mt-3 text-xs font-bold text-slate-400 transition hover:text-cyan-700 dark:text-slate-500 dark:hover:text-cyan-300"
              type="button"
              onClick={onDisconnect}
            >
              Change API key
            </button>
          </Show>
        </div>
      </aside>
      <div class="min-w-0 flex-1">
        <header class="flex h-16 items-center justify-between border-b border-slate-200 bg-white/90 px-5 backdrop-blur dark:border-slate-800 dark:bg-slate-950/90 lg:px-10">
          <div class="text-sm text-slate-500 dark:text-slate-400">
            <span class="hidden sm:inline">Lutra control plane</span>
            <span class="sm:hidden">Control plane</span>
          </div>
          <div class="flex items-center gap-2 text-xs font-semibold text-slate-500 dark:text-slate-400">
            <span
              class={["h-2 w-2 rounded-full", connected() ? "bg-emerald-500" : "bg-amber-400"]}
            />
            {connected() ? "Online" : "Setup required"}
          </div>
        </header>
        {props.children}
      </div>
    </div>
  );
}

export default function App() {
  return (
    <Router>
      {(props) => (
        <>
          <Title>Lutra Console</Title>
          <AppShell>
            <Errored
              fallback={(error, reset) => (
                <main class="mx-auto max-w-3xl px-5 py-10 lg:px-10">
                  <ErrorPanel
                    message={errorMessage(error(), "The console encountered an unexpected error.")}
                    onRetry={reset}
                  />
                </main>
              )}
            >
              {props.children}
            </Errored>
          </AppShell>
        </>
      )}
    </Router>
  );
}
