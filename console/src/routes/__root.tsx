import { createRootRoute, Outlet } from "@tanstack/react-router";
import App, { ErrorPage, NotFound } from "@/App";

export const Route = createRootRoute({
  component: () => (
    <App>
      <Outlet />
    </App>
  ),
  errorComponent: ErrorPage,
  notFoundComponent: NotFound,
});
