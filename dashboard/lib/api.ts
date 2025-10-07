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

export interface ManifestResponse {
  jobId: string;
  manifest: unknown;
  etag?: string;
  contentLength?: number;
}

export interface ResultLinkResponse {
  jobId: string;
  downloadUrl: string;
  key: string;
}

const API_BASE = (process.env.NEXT_PUBLIC_API_BASE_URL ?? '').replace(/\/$/, '');

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const base = API_BASE || 'http://localhost:8000';
  const response = await fetch(`${base}${path}`, {
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers || {}),
    },
    ...init,
  });

  if (!response.ok) {
    const payload = await safeJson(response);
    const errorMessage = (payload && (payload.detail || payload.message)) ?? response.statusText;
    throw new Error(errorMessage || `Request failed with ${response.status}`);
  }

  return (await response.json()) as T;
}

async function safeJson(response: Response): Promise<any> {
  try {
    return await response.json();
  } catch (error) {
    return null;
  }
}

export async function createUploadJob(): Promise<CreateJobResponse> {
  return apiFetch<CreateJobResponse>('/jobs', {
    method: 'POST',
    body: JSON.stringify({ source_type: 'upload' }),
  });
}

export async function createS3Job(s3Path: string): Promise<CreateJobResponse> {
  return apiFetch<CreateJobResponse>('/jobs', {
    method: 'POST',
    body: JSON.stringify({ source_type: 's3', s3_path: s3Path }),
  });
}

export async function fetchJob(jobId: string): Promise<JobStatusResponse> {
  return apiFetch<JobStatusResponse>(`/jobs/${jobId}`);
}

export async function fetchManifest(jobId: string): Promise<ManifestResponse> {
  return apiFetch<ManifestResponse>(`/jobs/${jobId}/manifest`);
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
  return apiFetch<ArtifactListResponse>(`/jobs/${jobId}/artifacts${suffix}`);
}

export async function fetchResultLink(jobId: string, path?: string): Promise<ResultLinkResponse> {
  const query = path ? `?path=${encodeURIComponent(path)}` : '';
  return apiFetch<ResultLinkResponse>(`/jobs/${jobId}/results${query}`);
}
