import { timestampMs as protoTimestampMs, type Timestamp } from "@bufbuild/protobuf/wkt";

export function timestampMs(timestamp: Timestamp | undefined): number | undefined {
  return timestamp === undefined ? undefined : protoTimestampMs(timestamp);
}

export function dateLabel(
  timestamp: Timestamp | undefined,
  options: Intl.DateTimeFormatOptions = { month: "short", day: "numeric", year: "numeric" },
): string {
  const value = timestampMs(timestamp);
  return value === undefined ? "—" : new Date(value).toLocaleDateString(undefined, options);
}

export function dateTimeLabel(timestamp: Timestamp | undefined): string {
  const value = timestampMs(timestamp);
  return value === undefined ? "—" : new Date(value).toLocaleString();
}

export function durationLabel(durationMs: bigint | number | undefined): string {
  if (durationMs === undefined) return "in progress";
  const milliseconds = Number(durationMs);
  return milliseconds < 1_000 ? `${milliseconds} ms` : `${(milliseconds / 1_000).toFixed(1)} s`;
}
