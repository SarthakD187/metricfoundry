// infra/main.ts
import { App } from "aws-cdk-lib";
import { MetricFoundryCoreStack } from "./stacks/core";
import { MetricFoundryApiStack } from "./stacks/api";

const app = new App();

const core = new MetricFoundryCoreStack(app, "MetricFoundry-Core");

new MetricFoundryApiStack(app, "MetricFoundry-Api", {
  artifactsBucket: core.artifacts,
  jobsTable: core.jobsTable,
  workflow: core.jobsStateMachine,
});
