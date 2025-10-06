// infra/stacks/api.ts (optional separate file; or fold into core later)
import { Stack, StackProps, Duration } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as apigw from "aws-cdk-lib/aws-apigatewayv2";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as ecr_assets from "aws-cdk-lib/aws-ecr-assets";
import * as path from "path";

export class MetricFoundryApiStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const asset = new ecr_assets.DockerImageAsset(this, "ApiImage", {
      directory: path.join(__dirname, "../../services/api"),
    });

    const fn = new lambda.DockerImageFunction(this, "ApiFn", {
      code: lambda.DockerImageCode.fromEcr(asset.repository, { tagOrDigest: asset.imageUri.split(":").pop()! }),
      memorySize: 512,
      timeout: Duration.seconds(10)
    });

    const http = new apigw.HttpApi(this, "HttpApi");
    http.addRoutes({
      path: "/{proxy+}",
      methods: [apigw.HttpMethod.ANY],
      integration: new integrations.HttpLambdaIntegration("ApiIntegration", fn),
    });
  }
}
