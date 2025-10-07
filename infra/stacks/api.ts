// infra/stacks/api.ts
import { Stack, StackProps, Duration } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as apigwv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";

interface MetricFoundryApiStackProps extends StackProps {
  artifactsBucket: any;
  jobsTable: any;
  jobsQueue?: any;
}

export class MetricFoundryApiStack extends Stack {
  constructor(scope: Construct, id: string, props: MetricFoundryApiStackProps) {
    super(scope, id, props);

    const { artifactsBucket, jobsTable, jobsQueue } = props;

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
    QUEUE_URL: jobsQueue ? jobsQueue.queueUrl : "",
  },
});

    // ------------------------------------------------------------------------
    // Permissions
    // ------------------------------------------------------------------------
    artifactsBucket.grantReadWrite(apiFn);
    jobsTable.grantReadWriteData(apiFn);

    if (jobsQueue) {
      apiFn.addToRolePolicy(
        new iam.PolicyStatement({
          actions: ["sqs:SendMessage"],
          resources: [jobsQueue.queueArn],
        })
      );
    }

    // ------------------------------------------------------------------------
    // HTTP API Gateway
    // ------------------------------------------------------------------------
    const httpApi = new apigwv2.HttpApi(this, "HttpApi", {
      apiName: "MetricFoundryApi",
      description: "FastAPI backend for MetricFoundry",
      corsPreflight: {
        allowOrigins: ["*"],
        allowMethods: [apigwv2.CorsHttpMethod.ANY],
      },
    });

    httpApi.addRoutes({
      path: "/{proxy+}",
      methods: [apigwv2.HttpMethod.ANY],
      integration: new integrations.HttpLambdaIntegration("ApiIntegration", apiFn),
    });

    // Output the API endpoint
    this.exportValue(httpApi.apiEndpoint, { name: "HttpApiUrl" });
  }
}
