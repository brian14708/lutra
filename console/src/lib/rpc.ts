import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { LutraService } from "../proto/lutra/v1/lutra_pb";

// In development the Vite server reverse-proxies /rpc to the Go server
// (see vite.config.ts). Production deployments serve the UI from the API
// server, so the same-origin path works there as well.
const transport = createConnectTransport({
  baseUrl: "/rpc",
});

export const lutraClient = createClient(LutraService, transport);
