// infra/stacks/api.ts
import { Stack, StackProps, Duration } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigwv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as authorizers from "aws-cdk-lib/aws-apigatewayv2-authorizers";

interface MetricFoundryApiStackProps extends StackProps {
  artifactsBucket: any;
  jobsTable: any;
  workflow: sfn.StateMachine;
  userPool: cognito.IUserPool;
  userPoolClient: cognito.IUserPoolClient;
  allowAnonymousJobCreation?: boolean;
}

export class MetricFoundryApiStack extends Stack {
  constructor(scope: Construct, id: string, props: MetricFoundryApiStackProps) {
    super(scope, id, props);

    const { artifactsBucket, jobsTable, workflow, userPool, userPoolClient } = props;
    const dashboardOrigin = process.env.FRONTEND_ORIGIN ?? "http://localhost:3000";
    const allowAnonymousJobCreation =
      props.allowAnonymousJobCreation ??
      (this.node.tryGetContext("allowAnonymousJobCreation") === true ||
        this.node.tryGetContext("allowAnonymousJobCreation") === "true");

    // ------------------------------------------------------------------------
    // API Lambda (FastAPI + Mangum)
    // ------------------------------------------------------------------------
    const apiFn = new lambda.Function(this, "ApiFn", {
      runtime: lambda.Runtime.PYTHON_3_11,   // ⬅️ switch from PYTHON_3_12
      architecture: lambda.Architecture.ARM_64,
      handler: "app.handler",
      code: lambda.Code.fromAsset("services/api"),
      timeout: Duration.seconds(29),
      environment: {
        BUCKET_NAME: artifactsBucket.bucketName,
        TABLE_NAME: jobsTable.tableName,
        STATE_MACHINE_ARN: workflow.stateMachineArn,
        FRONTEND_ORIGIN: dashboardOrigin,
        ALLOW_ANONYMOUS_JOB_CREATION: allowAnonymousJobCreation ? "true" : "false",
      },
    });

    // ------------------------------------------------------------------------
    // Permissions
    // ------------------------------------------------------------------------
    artifactsBucket.grantReadWrite(apiFn);
    jobsTable.grantReadWriteData(apiFn);
    workflow.grantStartExecution(apiFn);

    // ------------------------------------------------------------------------
    // HTTP API Gateway
    // ------------------------------------------------------------------------
    const httpApi = new apigwv2.HttpApi(this, "HttpApi", {
      apiName: "MetricFoundryApi",
      description: "FastAPI backend for MetricFoundry",
      corsPreflight: {
        allowOrigins: [dashboardOrigin],
        allowMethods: [apigwv2.CorsHttpMethod.ANY],
        allowHeaders: [
          "Authorization",
          "content-type",
          "x-amz-date",
          "x-amz-security-token",
          "x-amz-content-sha256",
        ],
        exposeHeaders: ["*"],
        allowCredentials: false,
      },
    });

    const integration = new integrations.HttpLambdaIntegration("ApiIntegration", apiFn);

    const cognitoAuthorizer = new authorizers.HttpUserPoolAuthorizer("JobsAuthorizer", userPool, {
      userPoolClients: [userPoolClient],
      identitySource: ["$request.header.Authorization"],
    });

    if (allowAnonymousJobCreation) {
      httpApi.addRoutes({
        path: "/jobs",
        methods: [apigwv2.HttpMethod.POST],
        integration,
      });
      httpApi.addRoutes({
        path: "/jobs",
        methods: [
          apigwv2.HttpMethod.GET,
          apigwv2.HttpMethod.PUT,
          apigwv2.HttpMethod.PATCH,
          apigwv2.HttpMethod.DELETE,
          apigwv2.HttpMethod.HEAD,
          apigwv2.HttpMethod.OPTIONS,
        ],
        integration,
        authorizer: cognitoAuthorizer,
      });
    } else {
      httpApi.addRoutes({
        path: "/jobs",
        methods: [apigwv2.HttpMethod.ANY],
        integration,
        authorizer: cognitoAuthorizer,
      });
    }

    httpApi.addRoutes({
      path: "/jobs/{proxy+}",
      methods: [apigwv2.HttpMethod.ANY],
      integration,
      authorizer: cognitoAuthorizer,
    });

    httpApi.addRoutes({
      path: "/health",
      methods: [apigwv2.HttpMethod.GET],
      integration,
    });

    httpApi.addRoutes({
      path: "/{proxy+}",
      methods: [apigwv2.HttpMethod.ANY],
      integration,
      authorizer: cognitoAuthorizer,
    });

    // Add new authenticated routes above this catch-all to keep them protected.

    // Output the API endpoint
    this.exportValue(httpApi.apiEndpoint, { name: "HttpApiUrl" });
  }
}
