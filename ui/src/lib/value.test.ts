import { expect, test } from "vitest";
import { encode as encodeCbor, Tagged } from "cborg";
import { decode, encode } from "@/lib/value.ts";

test("matches shared CBOR vectors", async () => {
  for (const [value, hex] of [
    [null, "f6"],
    [-1, "20"],
    [-0, "f98000"],
    [2n ** 64n, "c249010000000000000000"],
  ] as const) {
    expect(Buffer.from(encode(value)).toString("hex")).toBe(hex);
    expect(await decode(Buffer.from(hex, "hex"))).toEqual(value);
  }
});

test("roundtrips nested CBOR values", async () => {
  const values = [
    new Uint8Array([1, 2]),
    { answer: 42, items: [false, "ok"] },
    ["hello", { count: 2n ** 128n }],
  ];
  for (const value of values) expect(await decode(encode(value))).toEqual(value);
});

test("decodes and resolves blob references without interpreting their names", async () => {
  const uri = "blob:opaque-name";
  const bytes = encodeCbor(new Tagged(32, `${uri};resolve=true;mime=application%2Fcbor`));
  expect(
    await decode(bytes, async (requested) => {
      expect(requested).toBe(uri);
      return encode({ answer: 42 });
    }),
  ).toEqual({ answer: 42 });
  await expect(decode(bytes)).rejects.toThrow(/resolver required/);
  const unresolved = encodeCbor(new Tagged(32, uri));
  expect(await decode(unresolved)).toEqual({
    uri,
    resolve: false,
    mime_type: "application/octet-stream",
  });
});

test("resolves canonical blob and data URIs recursively", async () => {
  const digest = "LPJNul+wow4m6DsqxbninhsWHlwFP1hQGv5x6cXg8YI=";
  const uri = `blob:application/cbor,${digest}`;
  const inner = encodeCbor(new Tagged(32, "data:text/plain;base64;resolve,SGVsbG8="));
  const outer = encodeCbor(new Tagged(32, `blob:application/cbor;resolve,${digest}`));
  expect(
    await decode(outer, async (requested) => {
      expect(requested).toBe(uri);
      return inner;
    }),
  ).toEqual(new TextEncoder().encode("Hello"));
});
