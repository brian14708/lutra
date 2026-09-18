import type { Project } from "../proto/lutra/v1/project_pb";
import { projectClient } from "./rpc";
import type { Task } from "../proto/lutra/v1/task_pb";
import { taskClient } from "./rpc";
import type { Action, Run } from "../proto/lutra/v1/run_pb";
import { runClient } from "./rpc";

function required<T>(value: T | undefined, label: string): T {
  if (!value) throw new Error(`${label} was not returned by the control plane`);
  return value;
}

export type ProjectDashboard = {
  project: Project;
  tasks: Task[];
  runs: Run[];
};

export type RunDetails = {
  run: Run;
  actions: Action[];
};

export async function fetchProjects(): Promise<Project[]> {
  const response = await projectClient.listProjects({});
  return response.projects;
}

export async function fetchProjectDashboard(projectId: string): Promise<ProjectDashboard> {
  const [projectResponse, taskResponse, runResponse] = await Promise.all([
    projectClient.getProject({ projectId }),
    taskClient.listTasks({ projectId }),
    runClient.listRuns({ projectId }),
  ]);

  return {
    project: required(projectResponse.project, "Project"),
    tasks: taskResponse.tasks,
    runs: runResponse.runs,
  };
}

export async function fetchRunDetails(projectId: string, runId: string): Promise<RunDetails> {
  const [runResponse, actionResponse] = await Promise.all([
    runClient.getRun({ projectId, runId }),
    runClient.listActions({ projectId, runId }),
  ]);

  return {
    run: required(runResponse.run, "Run"),
    actions: actionResponse.actions,
  };
}

export async function cancelRun(projectId: string, runId: string): Promise<Run> {
  const response = await runClient.cancelRun({ projectId, runId });
  return required(response.run, "Canceled run");
}
