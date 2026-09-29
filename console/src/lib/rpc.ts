import { createConnectTransport } from "@connectrpc/connect-web";

// In development the Vite server reverse-proxies /api to the Go server
// (see vite.config.ts). Production deployments serve the UI from the API
// server, so the same-origin path works there as well.
export const transport = createConnectTransport({
  baseUrl: "/api",
  useHttpGet: true,
});
