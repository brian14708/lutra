import { Button } from "@kobalte/core/button";
import { createSignal } from "solid-js";

export default function Counter() {
  const [count, setCount] = createSignal(0);
  return (
    <Button
      class="rounded-lg bg-blue-600 px-6 py-2 text-base font-semibold text-white transition-colors hover:bg-blue-500 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600 active:bg-blue-700"
      onClick={() => setCount(count() + 1)}
    >
      Clicks: {count()}
    </Button>
  );
}
