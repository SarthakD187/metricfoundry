// ---- Types ----
export interface CreateJobResponse {
  jobId: string;
  uploadUrl?: string;
}

export interface JobStatusResponse {
  jobId: string;
  status: string;
  createdAt?: number;
  updatedAt?: number;
  resultKey?: string | null;
  source?: Record<string, unknown> | null;
  error?: string | null;
}

export interface ArtifactObjectSummary {
  key?: string;
  size?: number;
  lastModified?: string | null;
  etag?: string | null;
  storageClass?: string | null;
}

export interface ArtifactListResponse {
  jobId: string;
  prefix: string;
  objects: ArtifactObjectSummary[];
  commonPrefixes: (string | undefined)[];
  isTruncated: boolean;
  nextToken?: string | null;
}

export interface ManifestArtifactEntry {
  name?: string;
  description?: string;
  contentType?: string;
  key: string;
  [key: string]: unknown;
}

export interface ManifestPayload {
  jobId?: string;
  basePath?: string;
  artifacts?: ManifestArtifactEntry[];
  dataQuality?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface ManifestResponse {
  jobId: string;
  manifest: ManifestPayload;
  etag?: string;
  contentLength?: number;
}

export interface ResultLinkResponse {
  jobId: string;
  downloadUrl: string;
  key: string;
}

// ---- Config ----
// Prefer NEXT_PUBLIC_API_URL, fallback to NEXT_PUBLIC_API_BASE_URL.
// If neither is set at build-time, we later fall back to localhost for DX.
const rawBase =
  process.env.NEXT_PUBLIC_API_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  '';

const API_BASE = rawBase.replace(/\/$/, ''); // strip trailing slash for consistent joining

// Expose for quick debugging in browser DevTools: window.__API_BASE
if (typeof window !== 'undefined') {
  (window as any).__API_BASE = API_BASE || 'http://localhost:8000';
}

// Robust URL join that works with or without leading/trailing slashes.
function joinUrl(base: string, path: string): string {
  return new URL(path, base.endsWith('/') ? base : base + '/').toString();
}

// Safe JSON parse helper
async function safeJson(response: Response): Promise<any> {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

// Core fetch wrapper
async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  // DX-friendly fallback while keeping good URLs
  const base = API_BASE || 'http://localhost:8000';
  const url = joinUrl(base, path);

  let response: Response;
  try {
    response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...(init?.headers || {}),
      },
      ...init,
    });
  } catch (error) {
    const networkMessage =
      error instanceof TypeError
        ? `Unable to reach the MetricFoundry API at ${base}. Ensure the backend is running or set NEXT_PUBLIC_API_URL/NEXT_PUBLIC_API_BASE_URL.`
        : null;
    throw new Error(
      networkMessage ?? (error instanceof Error ? error.message : 'Request failed'),
    );
  }

  if (!response.ok) {
    const payload = await safeJson(response);
    const errorMessage =
      (payload && (payload.detail || payload.message)) ||
      `${response.status} ${response.statusText}`;
    throw new Error(errorMessage);
  }

  // If there is no JSON (204, etc.), return undefined as any
  try {
    return (await response.json()) as T;
  } catch {
    return undefined as unknown as T;
  }
}

// ---- API calls ----
export async function createUploadJob(): Promise<CreateJobResponse> {
  return apiFetch<CreateJobResponse>('jobs', {
    method: 'POST',
    body: JSON.stringify({ source_type: 'upload' }),
  });
}

export async function createS3Job(s3Path: string): Promise<CreateJobResponse> {
  return apiFetch<CreateJobResponse>('jobs', {
    method: 'POST',
    body: JSON.stringify({ source_type: 's3', s3_path: s3Path }),
  });
}

export async function fetchJob(jobId: string): Promise<JobStatusResponse> {
  return apiFetch<JobStatusResponse>(`jobs/${encodeURIComponent(jobId)}`);
}

export async function fetchManifest(jobId: string): Promise<ManifestResponse> {
  return apiFetch<ManifestResponse>(`jobs/${encodeURIComponent(jobId)}/manifest`);
}

export async function fetchArtifacts(
  jobId: string,
  params: { prefix?: string; continuationToken?: string; pageSize?: number; delimiter?: string },
): Promise<ArtifactListResponse> {
  const query = new URLSearchParams();
  if (params.prefix) query.set('prefix', params.prefix);
  if (params.continuationToken) query.set('continuationToken', params.continuationToken);
  if (params.pageSize) query.set('pageSize', String(params.pageSize));
  if (params.delimiter !== undefined) query.set('delimiter', params.delimiter);
  const suffix = query.toString() ? `?${query.toString()}` : '';
  return apiFetch<ArtifactListResponse>(`jobs/${encodeURIComponent(jobId)}/artifacts${suffix}`);
}

export async function fetchResultLink(jobId: string, path?: string): Promise<ResultLinkResponse> {
  const query = path ? `?path=${encodeURIComponent(path)}` : '';
  return apiFetch<ResultLinkResponse>(`jobs/${encodeURIComponent(jobId)}/results${query}`);
}
