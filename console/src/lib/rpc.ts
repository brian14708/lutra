import { createClient, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { ProjectService } from "../proto/lutra/v1/project_pb";
import { TaskService } from "../proto/lutra/v1/task_pb";
import { RunService } from "../proto/lutra/v1/run_pb";
import { apiKey } from "./session";

const authInterceptor: Interceptor = (next) => async (request) => {
  const key = apiKey();
  if (key) {
    request.header.set("Authorization", `Bearer ${key}`);
  }
  return next(request);
};

// In development the Vite server reverse-proxies /api to the Go server
// (see vite.config.ts). Production deployments serve the UI from the API
// server, so the same-origin path works there as well.
const transport = createConnectTransport({
  baseUrl: "/api",
  interceptors: [authInterceptor],
});

export const projectClient = createClient(ProjectService, transport);
export const taskClient = createClient(TaskService, transport);
export const runClient = createClient(RunService, transport);
