import { encode, type Value } from "@/lib/value";

export function encodeHelloParameters(input: string): Uint8Array {
  let parsed: unknown;
  try {
    parsed = JSON.parse(input);
  } catch {
    throw new Error("Enter valid JSON parameters.");
  }
  const cbor = encode(parsed as Value);
  if (cbor.length > 65536) throw new Error("Parameters must fit within 64 KiB.");
  return cbor;
}
