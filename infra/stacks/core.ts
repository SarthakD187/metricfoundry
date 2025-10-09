// infra/stacks/core.ts
import { Stack, StackProps, Duration, RemovalPolicy, CfnOutput } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as path from "path";
import { execSync } from "child_process";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";

/** Prefer local python/pip bundling to avoid Docker. Falls back to Docker if local fails. */
function pyLocalBundling(entryDir: string, extraDirs: string[] = []) {
  return {
    local: {
      tryBundle(outputDir: string) {
        try {
          // Ensure python is available
          execSync(`python3 --version`, { stdio: "ignore" });

          // pip install if requirements.txt exists
          const req = path.join(entryDir, "requirements.txt");
          execSync(
            `bash -lc 'if [ -f "${req}" ]; then python3 -m pip install --upgrade -r "${req}" -t "${outputDir}"; fi'`,
            { stdio: "inherit" }
          );

          // Copy the lambda source
          execSync(`rsync -a --exclude __pycache__/ "${entryDir}/" "${outputDir}/"`, {
            stdio: "inherit",
          });

          // Copy any extra directories (e.g., repo-local libs like services/)
          for (const extra of extraDirs) {
            const base = path.basename(extra);
            execSync(`rsync -a --exclude __pycache__/ "${extra}/" "${outputDir}/${base}/"`, {
              stdio: "inherit",
            });
          }

          return true; // bundled locally
        } catch {
          return false; // let CDK fall back to Docker
        }
      },
    },
    // Docker fallback IF local bundling returns false
    image: lambda.Runtime.PYTHON_3_11.bundlingImage,
    command: [
      "bash",
      "-c",
      'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output',
    ],
  };
}

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
    const stageCodePath = path.join(__dirname, "../../lambdas/stage");
    const stageFn = new lambda.Function(this, "StageSourceFn", {
      runtime: lambda.Runtime.PYTHON_3_11,
      // Ensure your file is lambdas/stage/handler.py with def lambda_handler(event, context)
      handler: "handler.lambda_handler",
      code: lambda.Code.fromAsset(stageCodePath, {
        bundling: pyLocalBundling(stageCodePath),
      }),
      timeout: Duration.minutes(10),
      memorySize: 2048,
      environment: {
        // Provide both names so code that expects either works
        JOBS_TABLE: this.jobsTable.tableName,
        TABLE_NAME: this.jobsTable.tableName,
        ARTIFACTS_BUCKET: this.artifacts.bucketName,
        BUCKET_NAME: this.artifacts.bucketName,
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
    const statusCodePath = path.join(__dirname, "../../lambdas/status");
    const statusFn = new lambda.Function(this, "StatusFn", {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: "handler.lambda_handler",
      code: lambda.Code.fromAsset(statusCodePath, {
        bundling: pyLocalBundling(statusCodePath),
      }),
      timeout: Duration.seconds(30),
      memorySize: 256,
      environment: {
        JOBS_TABLE: this.jobsTable.tableName,
        TABLE_NAME: this.jobsTable.tableName,
      },
    });

    this.jobsTable.grantReadWriteData(statusFn);

    // =====================================================================
    // Processor Lambda: runs analytics pipeline after staging completes
    // Copies repo-local `services/` into the bundle so imports work.
    // =====================================================================
    const processorCodePath = path.join(__dirname, "../../lambdas/processor");
    const repoRoot = path.join(__dirname, "../../");
    const servicesDir = path.join(repoRoot, "services");

    const processorFn = new lambda.DockerImageFunction(this, "ProcessorFn", {
  code: lambda.DockerImageCode.fromImageAsset("lambdas/processor"), // path directly to folder
  timeout: Duration.minutes(15),
  memorySize: 3008,
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
    stageTask.addCatch(markStageFailed.next(stageFailed), {
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

    // Helpful outputs
    new CfnOutput(this, "ArtifactsBucketName", {
      value: this.artifacts.bucketName,
      exportName: "MetricFoundryArtifactsBucketName",
    });
    new CfnOutput(this, "JobsTableName", {
      value: this.jobsTable.tableName,
      exportName: "MetricFoundryJobsTableName",
    });
    new CfnOutput(this, "JobsStateMachineArn", {
      value: this.jobsStateMachine.stateMachineArn,
      exportName: "MetricFoundryJobsStateMachineArn",
    });
  }
}
