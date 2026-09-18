import routes from "virtual:file-routes";
import { createRouter } from "@solidjs/router";
import { fileRoutes } from "@solidjs/router/fs";

// Keep route files independent. The project page and run page share a path
// prefix, but the project page is not a layout with an outlet; passing the
// flat manifest gives each file its own leaf match.
export const Router = createRouter({ routes: fileRoutes(routes) });
