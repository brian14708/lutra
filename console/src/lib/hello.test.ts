import { expect, test } from "vitest";
import { decode } from "@/lib/value";
import { encodeHelloParameters } from "@/lib/hello";

test("encodes editable JSON parameters as CBOR", async () => {
  expect(await decode(encodeHelloParameters('{"name":"Ada"}'))).toEqual({ name: "Ada" });
  expect(() => encodeHelloParameters("{bad")).toThrow(/valid JSON/);
  expect(() => encodeHelloParameters(JSON.stringify({ name: "a".repeat(65536) }))).toThrow(
    /64 KiB/,
  );
});
