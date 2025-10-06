// infra/stacks/api.ts
import { Stack, StackProps, Duration, CfnOutput } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as apigwv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as path from "path";

export interface ApiStackProps extends StackProps {
  artifacts: s3.Bucket;
  jobsQueue: sqs.Queue;
  jobsTable: ddb.Table;
}

export class MetricFoundryApiStack extends Stack {
  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);

    const assetPath = path.resolve(__dirname, "../../services/api");

    const fn = new lambda.DockerImageFunction(this, "ApiFn", {
  code: lambda.DockerImageCode.fromImageAsset(assetPath, { file: "Dockerfile" }),
  memorySize: 512,
  timeout: Duration.seconds(10),
  architecture: lambda.Architecture.ARM_64,   // <- add this line
  environment: {
    BUCKET_NAME: props.artifacts.bucketName,
    TABLE_NAME: props.jobsTable.tableName,
    QUEUE_URL: props.jobsQueue.queueUrl,
  },
});

    // Least-privilege grants
    props.artifacts.grantPut(fn);                  // allow uploads
    props.jobsTable.grantWriteData(fn);            // create job rows
    props.jobsQueue.grantSendMessages(fn);         // enqueue

    const http = new apigwv2.HttpApi(this, "HttpApi");
    http.addRoutes({
      path: "/{proxy+}",
      methods: [apigwv2.HttpMethod.ANY],
      integration: new integrations.HttpLambdaIntegration("ApiIntegration", fn),
    });

    new CfnOutput(this, "HttpApiUrl", { value: http.apiEndpoint });
  }
}
