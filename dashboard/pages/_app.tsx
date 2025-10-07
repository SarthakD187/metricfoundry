import type { AppProps } from 'next/app';
import Head from 'next/head';
import '../styles/globals.css';

export default function MetricFoundryApp({ Component, pageProps }: AppProps) {
  return (
    <>
      <Head>
        <title>MetricFoundry Dashboard</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </Head>
      <Component {...pageProps} />
    </>
  );
}
