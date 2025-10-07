// infra/stacks/core.ts
import { Stack, StackProps, Duration, RemovalPolicy } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";

export class MetricFoundryCoreStack extends Stack {
  public readonly artifacts: s3.Bucket;
  public readonly jobsQueue: sqs.Queue;
  public readonly jobsTable: ddb.Table;

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

    // --- SQS (existing, unused by the worker but kept as-is) ---
    const dlq = new sqs.Queue(this, "DLQ");
    this.jobsQueue = new sqs.Queue(this, "JobsQueue", {
      visibilityTimeout: Duration.seconds(90),
      deadLetterQueue: { queue: dlq, maxReceiveCount: 3 },
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
    // Worker Lambda: process uploads and write results + status to JobsTable
    // =====================================================================
    const processorFn = new lambda.Function(this, "ProcessorFn", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "handler.main",
      code: lambda.Code.fromAsset("lambdas/processor"),
      timeout: Duration.minutes(2),
      memorySize: 1024,
      environment: {
        JOBS_TABLE: this.jobsTable.tableName,
        ARTIFACTS_BUCKET: this.artifacts.bucketName,
      },
    });

    // Permissions for worker
    this.artifacts.grantReadWrite(processorFn);
    this.jobsTable.grantReadWriteData(processorFn);

    // S3 -> Lambda notification on new objects under artifacts/
    // (We filter to the top-level 'artifacts/' prefix here and enforce '/input/' in code)
    // Listen to ALL object-created events under artifacts/
this.artifacts.addEventNotification(
  s3.EventType.OBJECT_CREATED,               // <-- broader than PUT
  new s3n.LambdaDestination(processorFn),
  { prefix: "artifacts/" }
);

// Also listen under jobs/ (your API presigns here)
this.artifacts.addEventNotification(
  s3.EventType.OBJECT_CREATED,               // <-- broader than PUT
  new s3n.LambdaDestination(processorFn),
  { prefix: "jobs/" }
);

  }
}
