import { Title } from "@solidjs/meta";
import { Loading } from "solid-js";
import { paths, Router } from "./router";
import "./App.css";

// The app root: the router and the site-wide layout live here. Pages are
// the modules under src/routes.
export default function App() {
  return (
    <Router>
      {(props) => (
        <>
          <Title>Solid App</Title>
          <nav class="bg-slate-800 p-4 text-center">
            <a
              class="mx-0.5 inline-block rounded-lg px-3 py-1.5 font-semibold text-blue-300 no-underline transition-colors hover:bg-white/10 hover:text-blue-100"
              href={paths()}
            >
              Home
            </a>
            <a
              class="mx-0.5 inline-block rounded-lg px-3 py-1.5 font-semibold text-blue-300 no-underline transition-colors hover:bg-white/10 hover:text-blue-100"
              href={paths.users(1)}
            >
              Users
            </a>
          </nav>
          <Loading fallback={<main>Loading…</main>}>{props.children}</Loading>
        </>
      )}
    </Router>
  );
}
