// infra/stacks/core.ts
import { Stack, StackProps, Duration, RemovalPolicy } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as ssm from "aws-cdk-lib/aws-ssm";

export class MetricFoundryCoreStack extends Stack {
  public readonly artifacts: s3.Bucket;
  public readonly jobsTable: ddb.Table;
  public readonly jobsStateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // --- Artifacts bucket ---
    this.artifacts = new s3.Bucket(this, "Artifacts", {
      versioned: true,
      enforceSSL: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          transitions: [
            {
              storageClass: s3.StorageClass.INTELLIGENT_TIERING,
              transitionAfter: Duration.days(30),
            },
          ],
          expiration: Duration.days(180),
        },
      ],
      removalPolicy: RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
    });

    // --- Jobs table (pk/sk schema retained) ---
    this.jobsTable = new ddb.Table(this, "JobsTable", {
      partitionKey: { name: "pk", type: ddb.AttributeType.STRING },
      sortKey: { name: "sk", type: ddb.AttributeType.STRING },
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      removalPolicy: RemovalPolicy.RETAIN,
    });

    // =====================================================================
    // Stage Lambda: normalize source data into the artifacts bucket
    // =====================================================================
    const stageFn = new lambda.Function(this, "StageSourceFn", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "handler.handler",
      code: lambda.Code.fromAsset("lambdas/stage", {
        bundling: {
          image: lambda.Runtime.PYTHON_3_12.bundlingImage,
          command: [
            "bash",
            "-c",
            [
              "pip install -r requirements.txt -t /asset-output",
              "cp -au . /asset-output",
            ].join(" && "),
          ],
        },
      }),
      timeout: Duration.minutes(10),
      memorySize: 2048,
      environment: {
        JOBS_TABLE: this.jobsTable.tableName,
        ARTIFACTS_BUCKET: this.artifacts.bucketName,
      },
    });

    this.artifacts.grantReadWrite(stageFn);
    this.jobsTable.grantReadWriteData(stageFn);
    stageFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject"],
        resources: ["arn:aws:s3:::*/*"],
      })
    );

    // =====================================================================
    // Status Lambda: used by the workflow to record terminal failures
    // =====================================================================
    const statusFn = new lambda.Function(this, "StatusFn", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "handler.handler",
      code: lambda.Code.fromAsset("lambdas/status"),
      timeout: Duration.seconds(30),
      memorySize: 256,
      environment: {
        JOBS_TABLE: this.jobsTable.tableName,
      },
    });

    this.jobsTable.grantReadWriteData(statusFn);

    // =====================================================================
    // Processor Lambda: invoked by Step Functions after staging completes
    // =====================================================================
    const processorFn = new lambda.DockerImageFunction(this, "ProcessorFn", {
      code: lambda.DockerImageCode.fromImageAsset(".", {
        file: "lambdas/processor/Dockerfile",
      }),
      timeout: Duration.minutes(15),
      memorySize: 4096,
      environment: {
        JOBS_TABLE: this.jobsTable.tableName,
        ARTIFACTS_BUCKET: this.artifacts.bucketName,
      },
    });

    this.artifacts.grantReadWrite(processorFn);
    this.jobsTable.grantReadWriteData(processorFn);

    // =====================================================================
    // Step Functions state machine orchestrating staging + processing
    // =====================================================================
    const stageTask = new tasks.LambdaInvoke(this, "StageSource", {
      lambdaFunction: stageFn,
      payload: sfn.TaskInput.fromObject({
        jobId: sfn.JsonPath.stringAt("$.jobId"),
      }),
      resultPath: "$.stage",
      payloadResponseOnly: true,
    });

    stageTask.addRetry({
      errors: ["FileNotReadyError"],
      interval: Duration.seconds(20),
      backoffRate: 1.5,
      maxAttempts: 15,
    });

    const markStageFailed = new tasks.LambdaInvoke(this, "MarkStageFailed", {
      lambdaFunction: statusFn,
      payload: sfn.TaskInput.fromObject({
        jobId: sfn.JsonPath.stringAt("$.jobId"),
        error: sfn.JsonPath.stringAt("$.stageError.Cause"),
      }),
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
    });

    const stageFailed = new sfn.Fail(this, "StageFailed");
    const stageFailureChain = markStageFailed.next(stageFailed);

    stageTask.addCatch(stageFailureChain, {
      errors: ["States.ALL"],
      resultPath: "$.stageError",
    });

    const processTask = new tasks.LambdaInvoke(this, "ProcessJob", {
      lambdaFunction: processorFn,
      payload: sfn.TaskInput.fromObject({
        jobId: sfn.JsonPath.stringAt("$.jobId"),
        input: sfn.JsonPath.stringAt("$.stage.input"),
      }),
      resultPath: "$.process",
      payloadResponseOnly: true,
    });

    const markProcessFailed = new tasks.LambdaInvoke(this, "MarkProcessFailed", {
      lambdaFunction: statusFn,
      payload: sfn.TaskInput.fromObject({
        jobId: sfn.JsonPath.stringAt("$.jobId"),
        error: sfn.JsonPath.stringAt("$.processError.Cause"),
      }),
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
    });

    const processFailed = new sfn.Fail(this, "ProcessFailed");
    processTask.addCatch(markProcessFailed.next(processFailed), {
      errors: ["States.ALL"],
      resultPath: "$.processError",
    });

    this.jobsStateMachine = new sfn.StateMachine(this, "JobsStateMachine", {
      definition: stageTask.next(processTask),
      timeout: Duration.minutes(15),
    });

    // ------------------------------------------------------------------
    // Centralised configuration references in Parameter Store
    // ------------------------------------------------------------------
    new ssm.StringParameter(this, "ArtifactsBucketParameter", {
      parameterName: "/metricfoundry/storage/artifactsBucket",
      description: "Name of the S3 bucket storing MetricFoundry job artifacts",
      stringValue: this.artifacts.bucketName,
    });

    new ssm.StringParameter(this, "JobsTableParameter", {
      parameterName: "/metricfoundry/dynamodb/jobsTable",
      description: "DynamoDB table that tracks MetricFoundry jobs",
      stringValue: this.jobsTable.tableName,
    });

    new ssm.StringParameter(this, "JobsWorkflowParameter", {
      parameterName: "/metricfoundry/workflows/jobsStateMachineArn",
      description: "ARN of the Step Functions state machine that powers job execution",
      stringValue: this.jobsStateMachine.stateMachineArn,
    });
  }
}
