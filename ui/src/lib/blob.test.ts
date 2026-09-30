import { afterEach, expect, test, vi } from "vitest";
import type { Client } from "@connectrpc/connect";
import type { BlobService } from "@/proto/lutra/v1/blob_pb";
import { downloadBlob } from "@/lib/blob.ts";

afterEach(() => vi.unstubAllGlobals());

test("downloads a blob by URI", async () => {
  const digest = "LPJNul+wow4m6DsqxbninhsWHlwFP1hQGv5x6cXg8YI=";
  const uri = `blob:application/octet-stream,${digest}`;
  const bytes = new TextEncoder().encode("hello");
  const rpc = {
    async getDownload(request: { uri: string }) {
      expect(request.uri).toBe(uri);
      return { url: "https://s3.test/download" };
    },
  } as unknown as Client<typeof BlobService>;
  vi.stubGlobal("fetch", async (input: string | URL | Request) => {
    expect(input).toBe("https://s3.test/download");
    return new Response(new Uint8Array(bytes).buffer);
  });
  expect(await downloadBlob(rpc, uri)).toEqual(bytes);
  await expect(downloadBlob(rpc, "blob:text/plain,invalid")).rejects.toThrow(/digest/);
});

test("reports a failed blob download", async () => {
  const rpc = {
    async getDownload() {
      return { url: "https://s3.test/missing" };
    },
  } as unknown as Client<typeof BlobService>;
  vi.stubGlobal("fetch", async () => new Response(null, { status: 404 }));
  await expect(
    downloadBlob(rpc, "blob:text/plain,LPJNul+wow4m6DsqxbninhsWHlwFP1hQGv5x6cXg8YI="),
  ).rejects.toThrow(/HTTP 404/);
});
