import { Title } from "@solidjs/meta";
import { createMemo, Loading } from "solid-js";
import Counter from "../components/Counter";
import { lutraClient } from "../lib/rpc";
import logo from "../logo.svg";

export default function Home() {
  const pong = createMemo(
    async () => {
      try {
        return (await lutraClient.ping({ message: "ping" })).message;
      } catch (err) {
        return `error: ${err instanceof Error ? err.message : String(err)}`;
      }
    },
    { name: "pong" },
  );

  return (
    <main class="px-4 py-12 text-center">
      <Title>Home - Solid App</Title>
      <img src={logo} class="logo mx-auto h-[24vmin]" alt="Solid logo" />
      <h1>Hello Solid!</h1>
      <Counter />
      <p>
        RPC Ping: <Loading fallback="loading…">{pong()}</Loading>
      </p>
      <p>
        Edit <code>src/routes/index.tsx</code> and save to reload.
      </p>
      <a
        class="font-semibold text-sky-700 underline decoration-sky-300 decoration-2 underline-offset-2 transition-colors hover:text-sky-900"
        href="https://v2.solidjs.com/"
        target="_blank"
        rel="noopener noreferrer"
      >
        Learn Solid
      </a>
    </main>
  );
}
