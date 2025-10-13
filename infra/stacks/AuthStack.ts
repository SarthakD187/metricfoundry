// infra/stacks/AuthStack.ts
import { Stack, StackProps, CfnOutput, Duration, RemovalPolicy } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as cognito from "aws-cdk-lib/aws-cognito";

export interface MetricFoundryAuthStackProps extends StackProps {
  /** Optional domain prefix override for the Cognito hosted UI */
  readonly domainPrefix?: string;
  /** Optional callback/logout URLs for the hosted UI */
  readonly hostedUiCallbackUrls?: string[];
  readonly hostedUiLogoutUrls?: string[];
}

export class MetricFoundryAuthStack extends Stack {
  public readonly userPool: cognito.UserPool;
  public readonly userPoolClient: cognito.UserPoolClient;
  public readonly domain: cognito.UserPoolDomain;

  constructor(scope: Construct, id: string, props: MetricFoundryAuthStackProps = {}) {
    super(scope, id, props);

    const defaultCallback = process.env.FRONTEND_ORIGIN ?? "http://localhost:3000";
    const callbackUrls = props.hostedUiCallbackUrls ?? [defaultCallback];
    const logoutUrls = props.hostedUiLogoutUrls ?? [defaultCallback];

    this.userPool = new cognito.UserPool(this, "UserPool", {
      selfSignUpEnabled: true,
      signInAliases: { email: true, username: true },
      passwordPolicy: {
        minLength: 8,
        requireDigits: true,
        requireUppercase: false,
        requireLowercase: true,
        requireSymbols: false,
        tempPasswordValidity: Duration.days(7),
      },
      standardAttributes: {
        email: { required: true, mutable: true },
      },
      mfa: cognito.Mfa.OFF,
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
      removalPolicy: RemovalPolicy.RETAIN,
    });

    this.userPoolClient = this.userPool.addClient("AppClient", {
      authFlows: {
        userPassword: true,
        userSrp: true,
      },
      generateSecret: false,
      preventUserExistenceErrors: true,
      oAuth: {
        flows: {
          authorizationCodeGrant: true,
        },
        scopes: [
          cognito.OAuthScope.EMAIL,
          cognito.OAuthScope.OPENID,
          cognito.OAuthScope.PROFILE,
        ],
        callbackUrls,
        logoutUrls,
      },
      supportedIdentityProviders: [cognito.UserPoolClientIdentityProvider.COGNITO],
    });

    const derivedPrefix = this.deriveDomainPrefix(props.domainPrefix);
    this.domain = this.userPool.addDomain("UserPoolDomain", {
      cognitoDomain: { domainPrefix: derivedPrefix },
    });

    new CfnOutput(this, "UserPoolId", {
      value: this.userPool.userPoolId,
      exportName: "MetricFoundryUserPoolId",
    });

    new CfnOutput(this, "UserPoolClientId", {
      value: this.userPoolClient.userPoolClientId,
      exportName: "MetricFoundryUserPoolClientId",
    });

    new CfnOutput(this, "UserPoolDomain", {
      value: this.domain.domainName,
      exportName: "MetricFoundryUserPoolDomain",
    });
  }

  private deriveDomainPrefix(explicit?: string): string {
    if (explicit) {
      return this.sanitiseDomainPrefix(explicit);
    }
    const base = `${Stack.of(this).stackName}-${this.account ?? "acct"}-${this.region ?? "region"}`;
    const sanitized = this.sanitiseDomainPrefix(base);
    const suffix = this.node.addr.slice(-6);
    const merged = `${sanitized}-${suffix}`;
    return merged.slice(0, 63);
  }

  private sanitiseDomainPrefix(value: string): string {
    const lower = value.toLowerCase();
    const cleaned = lower.replace(/[^a-z0-9-]+/g, "-").replace(/^-+|-+$/g, "");
    return cleaned || "metricfoundry";
  }
}
