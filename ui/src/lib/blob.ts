/** Download blobs through Lutra presigned URLs. */
import type { Client } from "@connectrpc/connect";
import type { BlobService } from "@/proto/lutra/v1/blob_pb";

function decodeBase64(value: string): Uint8Array {
  if (!value || /[^A-Za-z0-9+/=]/.test(value) || value.length % 4 !== 0) {
    throw new Error("invalid blob URI digest");
  }
  let binary: string;
  try {
    binary = atob(value);
  } catch {
    throw new Error("invalid blob URI digest");
  }
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  let encoded = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    encoded += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  const canonical = btoa(encoded);
  if (canonical !== value) throw new Error("invalid blob URI digest");
  return bytes;
}

export function blobUriDigest(uri: string): Uint8Array {
  if (!uri.startsWith("blob:")) throw new Error("invalid blob URI");
  const name = uri.slice(5).split(",", 2).at(-1)!.split(";", 1).at(-1)!;
  const digest = decodeBase64(name);
  if (digest.length !== 32) throw new Error("blob URI digest must be SHA-256");
  return digest;
}

export async function downloadBlob(
  rpc: Client<typeof BlobService>,
  uri: string,
): Promise<Uint8Array> {
  blobUriDigest(uri);
  const signed = await rpc.getDownload({ uri });
  const response = await fetch(signed.url);
  if (!response.ok) throw new Error(`blob download failed: HTTP ${response.status}`);
  return new Uint8Array(await response.arrayBuffer());
}
