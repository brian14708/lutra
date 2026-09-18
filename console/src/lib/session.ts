import { createSignal } from "solid-js";

const STORAGE_KEY = "lutra_api_key";

function readStoredAPIKey(): string {
  if (typeof window === "undefined") return "";
  return window.localStorage.getItem(STORAGE_KEY) ?? "";
}

const [apiKey, setAPIKey] = createSignal(readStoredAPIKey());

export { apiKey };

export function saveAPIKey(value: string): void {
  const key = value.trim();
  if (!key || typeof window === "undefined") return;
  window.localStorage.setItem(STORAGE_KEY, key);
  setAPIKey(key);
}

export function clearAPIKey(): void {
  if (typeof window !== "undefined") window.localStorage.removeItem(STORAGE_KEY);
  setAPIKey("");
}
