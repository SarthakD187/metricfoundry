// infra/stacks/api.ts
import { Stack, StackProps, Duration } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigwv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as sns from "aws-cdk-lib/aws-sns";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as cloudwatchActions from "aws-cdk-lib/aws-cloudwatch-actions";

interface MetricFoundryApiStackProps extends StackProps {
  artifactsBucket: any;
  jobsTable: any;
  workflow: sfn.StateMachine;
}

export class MetricFoundryApiStack extends Stack {
  constructor(scope: Construct, id: string, props: MetricFoundryApiStackProps) {
    super(scope, id, props);

    const { artifactsBucket, jobsTable, workflow } = props;

    // ------------------------------------------------------------------------
    // API Lambda (FastAPI + Mangum)
    // ------------------------------------------------------------------------
    const rateLimitPerMinute = Number(this.node.tryGetContext("apiRateLimitPerMinute") ?? 120);
    const rateLimitBurst = Number(this.node.tryGetContext("apiRateLimitBurst") ?? rateLimitPerMinute);

    const apiKeySecret = new secretsmanager.Secret(this, "ApiKeySecret", {
      description: "API key used to protect the MetricFoundry HTTP API",
      generateSecretString: {
        secretStringTemplate: JSON.stringify({}),
        generateStringKey: "apiKey",
        passwordLength: 32,
        excludePunctuation: true,
      },
    });

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
        API_KEY_SECRET_ARN: apiKeySecret.secretArn,
        RATE_LIMIT_PER_MINUTE: String(rateLimitPerMinute),
        RATE_LIMIT_BURST: String(rateLimitBurst),
      },
    });

    apiKeySecret.grantRead(apiFn);

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
        allowOrigins: ["*"],
        allowMethods: [apigwv2.CorsHttpMethod.ANY],
      },
      createDefaultStage: false,
    });

    const routes = httpApi.addRoutes({
      path: "/{proxy+}",
      methods: [apigwv2.HttpMethod.ANY],
      integration: new integrations.HttpLambdaIntegration("ApiIntegration", apiFn),
    });

    const stage = new apigwv2.CfnStage(this, "HttpApiStage", {
      apiId: httpApi.apiId,
      stageName: "$default",
      autoDeploy: true,
      defaultRouteSettings: {
        throttlingBurstLimit: rateLimitBurst,
        throttlingRateLimit: rateLimitPerMinute / 60,
      },
    });

    for (const route of routes) {
      const cfnRoute = route.node.defaultChild as apigwv2.CfnRoute | undefined;
      if (cfnRoute) {
        stage.addDependency(cfnRoute);
      }
    }

    // Output the API endpoint
    this.exportValue(httpApi.apiEndpoint, { name: "HttpApiUrl" });

    new ssm.StringParameter(this, "ApiUrlParameter", {
      parameterName: "/metricfoundry/api/url",
      description: "Base URL for the MetricFoundry HTTP API",
      stringValue: httpApi.apiEndpoint,
    });

    new ssm.StringParameter(this, "ApiKeySecretArnParameter", {
      parameterName: "/metricfoundry/api/apiKeySecretArn",
      description: "Secrets Manager ARN that stores the MetricFoundry API key",
      stringValue: apiKeySecret.secretArn,
    });

    const alertsTopic = new sns.Topic(this, "AlertsTopic", {
      displayName: "MetricFoundry Alerts",
    });

    new ssm.StringParameter(this, "AlertsTopicArnParameter", {
      parameterName: "/metricfoundry/alerts/topicArn",
      description: "SNS topic ARN that receives MetricFoundry infrastructure alerts",
      stringValue: alertsTopic.topicArn,
    });

    const workflowFailureAlarm = new cloudwatch.Alarm(this, "WorkflowFailureAlarm", {
      metric: workflow.metricFailed({ period: Duration.minutes(1) }),
      threshold: 1,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      alarmDescription: "Alert when the job workflow reports a failure",
    });

    const apiErrorAlarm = new cloudwatch.Alarm(this, "ApiErrorAlarm", {
      metric: apiFn.metricErrors({ period: Duration.minutes(1) }),
      threshold: 1,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      alarmDescription: "Alert when the API Lambda reports execution errors",
    });

    const snsAction = new cloudwatchActions.SnsAction(alertsTopic);
    workflowFailureAlarm.addAlarmAction(snsAction);
    apiErrorAlarm.addAlarmAction(snsAction);

    this.exportValue(alertsTopic.topicArn, { name: "AlertsTopicArn" });
  }
}
