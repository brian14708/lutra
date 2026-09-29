import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import type { ErrorComponentProps } from "@tanstack/react-router";
import { Activity } from "lucide-react";

export default function App({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-[#f7f8fa] text-slate-800 md:grid md:grid-cols-[228px_minmax(0,1fr)]">
      <aside className="flex flex-col bg-[#17232b] px-4 py-3 text-slate-200 md:px-3.5 md:py-6">
        <Link
          className="flex items-center gap-3 px-2 py-1 text-[17px] font-bold md:mb-8"
          to="/"
          aria-label="Lutra Console home"
        >
          <span className="grid size-8 place-items-center rounded-md bg-teal-300 font-extrabold text-teal-950">
            L
          </span>
          <span>
            Lutra <small className="ml-1 text-[11px] font-medium text-slate-400">Console</small>
          </span>
        </Link>
        <div className="hidden px-3 pb-2 text-[10px] font-bold tracking-widest text-slate-400 uppercase md:block">
          Workspace
        </div>
        <nav aria-label="Main navigation" className="mt-2 md:mt-0">
          <Link
            to="/"
            activeOptions={{ exact: true }}
            className="flex items-center gap-3 rounded-md px-3 py-2.5 text-[13px] font-semibold text-slate-300 hover:bg-white/10 hover:text-white"
            activeProps={{ className: "bg-white/10 text-white" }}
          >
            <Activity size={18} aria-hidden="true" /> Overview
          </Link>
        </nav>
        <div className="mt-auto hidden border-t border-white/10 px-3 pt-4 text-xs text-slate-400 md:block">
          Lutra
        </div>
      </aside>
      <div className="min-w-0">
        <header className="flex h-[61px] items-center justify-between border-b border-slate-200 bg-white px-5 text-[13px] font-semibold md:px-10">
          <span>Console</span>
          <span className="text-[11px] font-medium text-slate-500">Local environment</span>
        </header>
        {children}
      </div>
    </div>
  );
}

export function NotFound() {
  return (
    <main className="mx-auto max-w-[1050px] px-5 py-8 md:px-10 md:py-11">
      <p className="mb-2 text-[11px] font-bold tracking-widest text-teal-700 uppercase">404</p>
      <h1 className="text-[29px] font-bold">Page not found</h1>
      <p className="mt-2 text-[13px] text-slate-500">The page you requested does not exist.</p>
      <Link
        className="mt-5 inline-block text-[13px] font-semibold text-teal-700 hover:underline"
        to="/"
      >
        Return to overview
      </Link>
    </main>
  );
}

export function ErrorPage({ error, reset }: ErrorComponentProps) {
  const message = error instanceof Error ? error.message : String(error);
  return (
    <main className="mx-auto mt-[12vh] max-w-xl p-8">
      <h1 className="text-2xl font-bold">Unable to load the console</h1>
      <p className="my-4 text-sm text-slate-500">{message}</p>
      <button
        className="rounded-md bg-teal-700 px-3 py-2 text-xs font-semibold text-white hover:bg-teal-800"
        type="button"
        onClick={reset}
      >
        Try again
      </button>
    </main>
  );
}
