import { App } from "aws-cdk-lib";
import { MetricFoundryCoreStack } from "./stacks/core";
import { MetricFoundryApiStack } from "./stacks/api";

const app = new App();
const env = { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION };

const core = new MetricFoundryCoreStack(app, "MetricFoundry-Core", { env });
new MetricFoundryApiStack(app, "MetricFoundry-Api", {
  env,
  artifacts: core.artifacts,
  jobsQueue: core.jobsQueue,
  jobsTable: core.jobsTable,
});
