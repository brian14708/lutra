/** CBOR values and Lutra blob references. */
import { decode as decodeCbor, encode as encodeCbor, Tagged, Token, Type } from "cborg";
import { bigIntDecoder, bigIntEncoder, bigNegIntDecoder } from "cborg/taglib";

export interface BlobRef {
  uri: string;
  resolve: boolean;
  mime_type: string;
}
export type Value =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | BlobRef
  | Value[]
  | { [key: string]: Value };

export function failureMessage(bytes: Uint8Array): string {
  if (!bytes.length) return "";
  const value: unknown = decodeCbor(bytes, {
    tags: {
      2: bigIntDecoder,
      3: bigNegIntDecoder,
      32: Tagged.decoder(32),
      30001: Tagged.decoder(30001),
    },
  });
  if (!(value instanceof Tagged) || value.tag !== 30001) return "";
  const fields: unknown = value.value;
  if (
    fields === null ||
    typeof fields !== "object" ||
    !("cacheable" in fields) ||
    typeof fields.cacheable !== "boolean" ||
    !("message" in fields) ||
    typeof fields.message !== "string" ||
    !fields.message ||
    !("details" in fields)
  ) {
    throw new Error("invalid result failure");
  }
  return fields.message;
}

export function encode(value: Value): Uint8Array {
  return encodeCbor(value, {
    typeEncoders: {
      number: (number: number) => (Object.is(number, -0) ? new Token(Type.float, number) : null),
      bigint: bigIntEncoder,
    },
  });
}

async function resolve(
  value: unknown,
  resolver?: (uri: string) => Promise<Uint8Array>,
): Promise<Value> {
  if (value instanceof Tagged) {
    if (value.tag !== 32 || typeof value.value !== "string")
      throw new Error("unsupported CBOR tag");
    const raw = value.value;
    const comma = raw.indexOf(",");
    const head = comma < 0 ? raw : raw.slice(0, comma);
    const payload = comma < 0 ? "" : raw.slice(comma);
    const [schemeAndMime, ...attrs] = head.split(";");
    if (!schemeAndMime.startsWith("blob:") && !schemeAndMime.startsWith("data:"))
      throw new Error("invalid blob reference");
    const options = Object.fromEntries(
      attrs.filter((item) => item.includes("=")).map((item) => item.split("=", 2)),
    );
    const retained = attrs.filter(
      (item) => item !== "resolve" && !item.startsWith("resolve=") && !item.startsWith("mime="),
    );
    const uri = [schemeAndMime, ...retained].join(";") + payload;
    const inferred =
      schemeAndMime.startsWith("blob:") && comma < 0
        ? "application/octet-stream"
        : schemeAndMime.slice(schemeAndMime.indexOf(":") + 1);
    const mime_type = decodeURIComponent(options.mime ?? (inferred || "text/plain"));
    const ref = {
      uri,
      resolve: attrs.includes("resolve") || options.resolve === "true",
      mime_type,
    };
    if (!ref.resolve) return ref;
    let data: Uint8Array;
    if (uri.startsWith("data:")) {
      const meta = uri.slice(5, uri.indexOf(","));
      const body = uri.slice(uri.indexOf(",") + 1);
      data = meta.endsWith(";base64")
        ? Uint8Array.from(atob(body), (char) => char.charCodeAt(0))
        : Uint8Array.from(decodeURIComponent(body), (char) => char.charCodeAt(0));
    } else {
      if (!resolver) throw new Error("blob resolver required");
      data = await resolver(uri);
    }
    return mime_type === "application/cbor" ? decode(data, resolver) : data;
  }
  if (Array.isArray(value)) return Promise.all(value.map((item) => resolve(item, resolver)));
  if (value && typeof value === "object" && !(value instanceof Uint8Array))
    return Object.fromEntries(
      await Promise.all(
        Object.entries(value).map(
          async ([key, item]) => [key, await resolve(item, resolver)] as const,
        ),
      ),
    );
  return value as Value;
}

export async function decode(
  bytes: Uint8Array,
  resolver?: (uri: string) => Promise<Uint8Array>,
): Promise<Value> {
  return resolve(
    decodeCbor(bytes, { tags: { 2: bigIntDecoder, 3: bigNegIntDecoder, 32: Tagged.decoder(32) } }),
    resolver,
  );
}
