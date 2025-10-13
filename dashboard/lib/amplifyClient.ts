// dashboard/lib/amplifyClient.ts
import { Amplify } from 'aws-amplify';
import { signInWithRedirect } from 'aws-amplify/auth';

let configured = false;

export function isAuthConfigured(): boolean {
  return !!(
    process.env.NEXT_PUBLIC_COGNITO_USER_POOL_ID &&
    process.env.NEXT_PUBLIC_COGNITO_USER_POOL_CLIENT_ID
  );
}

function resolveRedirect(defaultUrl: string | undefined): string {
  if (defaultUrl && defaultUrl.trim()) {
    return defaultUrl.trim();
  }
  if (typeof window !== 'undefined' && window.location?.origin) {
    return window.location.origin;
  }
  return 'http://localhost:3000';
}

export function ensureAmplifyConfigured(): void {
  if (configured) return;

  const region =
    process.env.NEXT_PUBLIC_AWS_REGION ??
    process.env.NEXT_PUBLIC_COGNITO_REGION ??
    process.env.NEXT_PUBLIC_REGION ??
    'us-east-1';
  const authAvailable = isAuthConfigured();
  const userPoolId = process.env.NEXT_PUBLIC_COGNITO_USER_POOL_ID;
  const userPoolClientId = process.env.NEXT_PUBLIC_COGNITO_USER_POOL_CLIENT_ID;
  const domain = process.env.NEXT_PUBLIC_COGNITO_DOMAIN;

  if (!authAvailable || !userPoolId || !userPoolClientId) {
    console.warn('Cognito user pool environment variables are not set; authentication disabled.');
    configured = true;
    return;
  }

  const redirectSignIn = resolveRedirect(process.env.NEXT_PUBLIC_OAUTH_REDIRECT_SIGN_IN);
  const redirectSignOut = resolveRedirect(process.env.NEXT_PUBLIC_OAUTH_REDIRECT_SIGN_OUT);

  Amplify.configure({
    Auth: {
      Cognito: {
        userPoolId,
        userPoolClientId,
        region,
        loginWith: domain
          ? {
              oauth: {
                domain,
                scopes: ['openid', 'email', 'profile'],
                redirectSignIn,
                redirectSignOut,
                responseType: 'code',
              },
            }
          : undefined,
      },
    },
  });

  configured = true;
}

export async function startHostedUiSignIn(): Promise<void> {
  if (typeof window === 'undefined') return;
  if (!isAuthConfigured()) return;
  ensureAmplifyConfigured();
  try {
    await signInWithRedirect();
  } catch (error) {
    console.error('Failed to start hosted UI sign-in redirect', error);
  }
}
