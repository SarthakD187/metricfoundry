// infra/stacks/core.ts
import { Stack, StackProps, Duration, RemovalPolicy } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as ddb from "aws-cdk-lib/aws-dynamodb";

export class MetricFoundryCoreStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const artifacts = new s3.Bucket(this, "Artifacts", {
      versioned: true,
      enforceSSL: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [{ transitions: [{ storageClass: s3.StorageClass.INTELLIGENT_TIERING, transitionAfter: Duration.days(30) }], expiration: Duration.days(180) }],
      removalPolicy: RemovalPolicy.RETAIN,
      autoDeleteObjects: false
    });

    const jobs = new sqs.Queue(this, "JobsQueue", {
      visibilityTimeout: Duration.seconds(90),
      deadLetterQueue: { queue: new sqs.Queue(this, "DLQ"), maxReceiveCount: 3 }
    });

    const table = new ddb.Table(this, "JobsTable", {
      partitionKey: { name: "pk", type: ddb.AttributeType.STRING },
      sortKey: { name: "sk", type: ddb.AttributeType.STRING },
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      removalPolicy: RemovalPolicy.RETAIN
    });

    // TODO: add API + Lambda after we wire FastAPI container
  }
}
