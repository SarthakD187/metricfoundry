// dashboard/lib/api.ts

// ---- Types ----
export interface CreateJobResponse {
  jobId: string;
  uploadUrl?: string;
  source?: Record<string, unknown> | null;
}

export type JobStatus = "CREATED" | "QUEUED" | "RUNNING" | "SUCCEEDED" | "FAILED";

export interface JobStatusResponse {
  jobId: string;
  status: JobStatus;
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
  key?: string;
  name?: string;
  description?: string;
  contentType?: string;
  size?: number;
  checksum?: string;
  [key: string]: unknown;
}

export interface ManifestPayload {
  basePath?: string;
  artifacts?: ManifestArtifactEntry[];
  dataQuality?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface ManifestResponse {
  jobId: string;
  manifest: ManifestPayload | null;
  etag?: string | null;
  contentLength?: number | null;
}

export interface ResultLinkResponse {
  jobId: string;
  downloadUrl: string;
  key: string;
}

export interface ResultsJson {
  jobId?: string;
  summary?: { rows?: number; columns?: number; [k: string]: unknown };
  schema?: Array<{ name: string; type?: string }>;
  links?: { manifest?: string; resultsManifest?: string; input?: string; reportHtml?: string; [k: string]: unknown };
  generatedAt?: string;
  [k: string]: unknown;
}

// ---- Config ----
const rawBase =
  process.env.NEXT_PUBLIC_API_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "";

const API_BASE = rawBase.replace(/\/$/, "");

if (typeof window !== "undefined") {
  (window as any).__API_BASE = API_BASE || "http://127.0.0.1:8000";
}

function joinUrl(base: string, path: string): string {
  return new URL(path, base.endsWith("/") ? base : base + "/").toString();
}

async function safeJson(response: Response): Promise<any> {
  try { return await response.json(); } catch { return null; }
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const base = API_BASE || "http://127.0.0.1:8000";
  const url = joinUrl(base, path);
  let response: Response;
  try {
    response = await fetch(url, {
      headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
      ...init,
    });
  } catch (error) {
    const networkMessage =
      error instanceof TypeError
        ? `Unable to reach API at ${base}. Is it running?`
        : null;
    throw new Error(networkMessage ?? (error instanceof Error ? error.message : "Request failed"));
  }
  if (!response.ok) {
    const payload = await safeJson(response);
    const msg = (payload && (payload.detail || payload.message)) || `${response.status} ${response.statusText}`;
    throw new Error(msg);
  }
  try { return (await response.json()) as T; } catch { return undefined as unknown as T; }
}

// ---- API calls ----
export async function createUploadJob(file: File | Blob): Promise<CreateJobResponse> {
  const sourceConfig: Record<string, unknown> = {};
  if (file instanceof File && file.name) {
    sourceConfig.filename = file.name;
  }
  if (typeof file.type === "string" && file.type.trim() !== "") {
    sourceConfig.contentType = file.type;
  }

  const payload: Record<string, unknown> = { source_type: "upload" };
  if (Object.keys(sourceConfig).length > 0) {
    payload.source_config = sourceConfig;
  }

  return apiFetch<CreateJobResponse>("jobs", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function fetchJob(jobId: string): Promise<JobStatusResponse> {
  return apiFetch<JobStatusResponse>(`jobs/${encodeURIComponent(jobId)}`);
}

export async function fetchResultLink(jobId: string, path?: string): Promise<ResultLinkResponse> {
  const query = path ? `?path=${encodeURIComponent(path)}` : "";
  return apiFetch<ResultLinkResponse>(`jobs/${encodeURIComponent(jobId)}/results${query}`);
}

export interface ListArtifactsOptions {
  prefix?: string;
  continuationToken?: string | null;
  pageSize?: number;
  delimiter?: string | null;
}

export async function listArtifacts(
  jobId: string,
  options?: string | ListArtifactsOptions,
): Promise<ArtifactListResponse> {
  const opts: ListArtifactsOptions =
    typeof options === "string" ? { prefix: options } : options ?? {};

  const q = new URLSearchParams();
  if (opts.prefix) q.set("prefix", opts.prefix);
  if (opts.continuationToken) q.set("continuationToken", opts.continuationToken);
  if (typeof opts.pageSize === "number") q.set("pageSize", String(opts.pageSize));
  if (opts.delimiter !== undefined) q.set("delimiter", opts.delimiter ?? "");

  const query = q.toString();
  const suffix = query ? `?${query}` : "";
  return apiFetch<ArtifactListResponse>(`jobs/${encodeURIComponent(jobId)}/artifacts${suffix}`);
}

export async function fetchManifest(jobId: string): Promise<ManifestResponse> {
  return apiFetch<ManifestResponse>(`jobs/${encodeURIComponent(jobId)}/manifest`);
}

export async function presign(jobId: string, key: string) {
  const q = new URLSearchParams({ key });
  return apiFetch<ResultLinkResponse>(`jobs/${encodeURIComponent(jobId)}/download?${q}`);
}

export async function processJobNow(jobId: string): Promise<{ ok: boolean; resultKey?: string }> {
  return apiFetch<{ ok: boolean; resultKey?: string }>(`jobs/${encodeURIComponent(jobId)}/process`, { method: "POST" });
}

export async function uploadToPresigned(url: string, file: File | Blob): Promise<void> {
  const headers: Record<string, string> = {};
  if (typeof file.type === "string" && file.type.trim() !== "") {
    headers["content-type"] = file.type;
  }
  const r = await fetch(url, { method: "PUT", headers, body: file });
  if (!r.ok) throw new Error(`Upload failed: ${r.status} ${r.statusText}`);
}

export async function fetchResultsJson(jobId: string): Promise<{ meta: ResultLinkResponse; json: ResultsJson }> {
  const meta = await fetchResultLink(jobId);
  const r = await fetch(meta.downloadUrl);
  if (!r.ok) throw new Error(`Failed to download results.json (${r.status})`);
  const json = (await r.json()) as ResultsJson;
  return { meta, json };
}
