// infra/main.ts
import { App } from "aws-cdk-lib";
import { MetricFoundryCoreStack } from "./stacks/core";
import { MetricFoundryApiStack } from "./stacks/api";
import { MetricFoundryAuthStack } from "./stacks/AuthStack";

const app = new App();

const core = new MetricFoundryCoreStack(app, "MetricFoundry-Core");
const auth = new MetricFoundryAuthStack(app, "MetricFoundry-Auth");

new MetricFoundryApiStack(app, "MetricFoundry-Api", {
  artifactsBucket: core.artifacts,
  jobsTable: core.jobsTable,
  workflow: core.jobsStateMachine,
  userPool: auth.userPool,
  userPoolClient: auth.userPoolClient,
});
