// dashboard/pages/index.tsx

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import Head from "next/head";
import UploadForm from "../components/UploadForm";
import JobCard from "../components/JobCard";
import ResultsViewer from "../components/ResultsViewer";
import {
  fetchJob,
  fetchResultsJson,
  processJobNow,
  uploadToPresigned,
  createUploadJob,
  type JobStatusResponse,
  type ResultsJson,
} from "../lib/api";
import { TrackedJob, usePersistentJobs } from "../hooks/usePersistentJobs";

const POLL_INTERVAL = 5000;

export default function DashboardHome() {
  const { jobs, addJob, updateJob, removeJob } = usePersistentJobs([]);
  const [globalError, setGlobalError] = useState<string | null>(null);
  const [existingJobId, setExistingJobId] = useState("");
  const [mounted, setMounted] = useState(false);

  // Latest finished result shown as a rich panel
  const [activeResult, setActiveResult] = useState<{
    jobId: string;
    json: ResultsJson;
    downloadUrl: string;
  } | null>(null);

  useEffect(() => setMounted(true), []);

  const refreshJob = useCallback(
    async (jobId: string) => {
      try {
        const response = await fetchJob(jobId);
        updateJob(jobId, {
          jobId: response.jobId,
          status: response.status,
          createdAt: response.createdAt,
          updatedAt: response.updatedAt,
          resultKey: response.resultKey ?? null,
          sourceType:
            typeof response.source === "object" && (response as any).source?.type
              ? String((response as any).source.type)
              : undefined,
          error: (response as any).error ?? null,
        });

        if (response.status === "SUCCEEDED") {
          try {
            const { meta, json } = await fetchResultsJson(jobId);
            setActiveResult({ jobId, json, downloadUrl: meta.downloadUrl });
            updateJob(jobId, { resultKey: meta.key });
          } catch {
            // ignore races; next poll will catch it
          }
        }
      } catch (error) {
        console.error("Failed to refresh job", error);
        const message = error instanceof Error ? error.message : "Unable to refresh job";
        setGlobalError(message);
      }
    },
    [updateJob],
  );

  // Local polling helper (replaces pollUntilComplete)
  const waitForCompletion = useCallback(
    async (
      jobId: string,
      opts?: {
        intervalMs?: number;
        onTick?: (j: JobStatusResponse) => void;
        timeoutMs?: number;
      },
    ): Promise<JobStatusResponse> => {
      const interval = Math.max(500, opts?.intervalMs ?? 1500);
      const timeout = opts?.timeoutMs ?? 5 * 60 * 1000; // 5 minutes
      const start = Date.now();

      // eslint-disable-next-line no-constant-condition
      while (true) {
        const j = await fetchJob(jobId);
        opts?.onTick?.(j);
        if (j.status === "SUCCEEDED" || j.status === "FAILED") return j;

        if (Date.now() - start > timeout) {
          return { ...j, status: "FAILED", error: "Timed out waiting for completion" };
        }
        await new Promise((r) => setTimeout(r, interval));
      }
    },
    [],
  );

  const handleJobCreated = useCallback(async () => {
    setGlobalError(null);
    try {
      const resp = await createUploadJob();
      const jobId = resp.jobId;
      addJob({
        jobId,
        status: "CREATED",
        sourceType: "upload",
        uploadState: "uploading",
      });

      // The UploadForm will call back with the actual file to PUT;
      // we return the tuple needed for it to continue.
      return { jobId, uploadUrl: resp.uploadUrl! };
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Failed to create job";
      setGlobalError(msg);
      throw e;
    }
  }, [addJob]);

  const handleUpload = useCallback(
    async (jobId: string, _file: File | Blob) => {
      setGlobalError(null);
      try {
        updateJob(jobId, { uploadState: "uploading" });
      } catch (e) {
        const msg = e instanceof Error ? e.message : "Upload failed";
        setGlobalError(msg);
        throw e;
      }
    },
    [updateJob],
  );

  const handleUploadFinished = useCallback(
    async (jobId: string, _presignedUrlUsed: string) => {
      setGlobalError(null);
      try {
        updateJob(jobId, { uploadState: "uploaded" });

        // DEV: immediately trigger processing
        await processJobNow(jobId);

        // Poll until finished (use local helper)
        const final: JobStatusResponse = await waitForCompletion(jobId, {
          intervalMs: 1500,
          onTick: (j) => {
            updateJob(jobId, {
              status: j.status,
              updatedAt: j.updatedAt,
              resultKey: j.resultKey ?? null,
              error: j.error ?? null,
            });
          },
        });

        if (final.status === "SUCCEEDED") {
          const { meta, json } = await fetchResultsJson(jobId);
          setActiveResult({ jobId, json, downloadUrl: meta.downloadUrl });
          updateJob(jobId, { resultKey: meta.key, status: "SUCCEEDED" });
        } else {
          setGlobalError(final.error || "Processing failed");
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : "Processing failed";
        setGlobalError(msg);
      } finally {
        refreshJob(jobId);
      }
    },
    [refreshJob, updateJob, waitForCompletion],
  );

  const handleExistingJobSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!existingJobId.trim()) return;
    const jobId = existingJobId.trim();
    try {
      const response = await fetchJob(jobId);
      addJob({
        jobId,
        status: response.status,
        createdAt: response.createdAt,
        updatedAt: response.updatedAt,
        resultKey: response.resultKey ?? null,
        sourceType:
          typeof response.source === "object" && (response as any).source?.type
            ? String((response as any).source.type)
            : undefined,
      });
      setExistingJobId("");

      if (response.status === "SUCCEEDED") {
        try {
          const { meta, json } = await fetchResultsJson(jobId);
          setActiveResult({ jobId, json, downloadUrl: meta.downloadUrl });
          updateJob(jobId, { resultKey: meta.key });
        } catch {
          /* ignore */
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to find job";
      setGlobalError(message);
    }
  };

  const activeJobIds = useMemo(
    () =>
      jobs
        .filter((job) => {
          const status = job.status?.toUpperCase?.() ?? "UNKNOWN";
          return !["SUCCEEDED", "FAILED"].includes(status);
        })
        .map((job) => job.jobId),
    [jobs],
  );

  useEffect(() => {
    if (!mounted || activeJobIds.length === 0) return;
    const interval = setInterval(() => {
      activeJobIds.forEach((jobId) => {
        refreshJob(jobId);
      });
    }, POLL_INTERVAL);
    return () => clearInterval(interval);
  }, [mounted, activeJobIds.join(","), refreshJob]);

  return (
    <>
      <Head>
        <title>MetricFoundry Dashboard</title>
      </Head>
      <main className="page">
        <section className="hero">
          <div className="hero-content">
            <span className="hero-badge">MetricFoundry</span>
            <h1>Operational console for your data workflows</h1>
            <p>
              Upload datasets, orchestrate ingestion jobs, and view rich, user-friendly analyses of the results.
            </p>
          </div>
          <div className="hero-panels">
            <UploadForm
              onJobCreated={async () => {
                const { jobId, uploadUrl } = await handleJobCreated();
                return {
                  jobId,
                  // Provide a concrete uploader so the component can PUT the file immediately:
                  upload: async (file: File | Blob) => {
                    await uploadToPresigned(uploadUrl, file);
                    await handleUpload(jobId, file);
                    await handleUploadFinished(jobId, uploadUrl);
                  },
                };
              }}
              onError={(message) => setGlobalError(message)}
            />
            <form className="glass-panel inline-form" onSubmit={handleExistingJobSubmit}>
              <p className="eyebrow">Track an existing job</p>
              <h2>Already have a job ID?</h2>
              <p className="subtitle">Paste it below to monitor and load outputs.</p>
              <div className="form-grid">
                <label className="field">
                  <span>Job ID</span>
                  <input
                    type="text"
                    value={existingJobId}
                    onChange={(event) => setExistingJobId(event.target.value)}
                    placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                  />
                </label>
              </div>
              <div className="form-actions">
                <button type="submit">Add job</button>
              </div>
            </form>
          </div>
        </section>

        {globalError && <div className="error-banner global">{globalError}</div>}

        {activeResult && (
          <section className="result-banner">
            <div className="glass-panel">
              <p className="eyebrow">Latest analysis</p>
              <h3>Results for job {activeResult.jobId}</h3>
              <ResultsViewer
                jobId={activeResult.jobId}
                data={activeResult.json}
                downloadUrl={activeResult.downloadUrl}
              />
            </div>
          </section>
        )}

        <section className="jobs-section">
          <div className="section-heading">
            <div>
              <p className="eyebrow">Jobs</p>
              <h2>Recent activity</h2>
            </div>
            <p className="subtitle">
              Jobs are stored locally in your browser for convenience. Remove entries here without affecting backend data.
            </p>
          </div>

          {!mounted ? (
            <div className="empty-state">
              <h3>Loading…</h3>
              <p>Preparing your recent jobs.</p>
            </div>
          ) : jobs.length === 0 ? (
            <div className="empty-state">
              <h3>No jobs yet</h3>
              <p>Create a job above to see it appear here.</p>
            </div>
          ) : (
            <div className="jobs-grid">
              {jobs.map((job) => (
                <JobCard key={job.jobId} job={job} onRefresh={refreshJob} onRemove={removeJob} />
              ))}
            </div>
          )}
        </section>
      </main>

      <style jsx>{`
        .page { padding: 1.25rem; }
        .hero { display: grid; grid-template-columns: 1.2fr 1fr; gap: 1rem; margin-bottom: 1rem; }
        .hero-badge { font-size: 0.75rem; opacity: 0.6; }
        .hero-panels { display: grid; gap: 1rem; }
        .glass-panel {
          border: 1px solid rgba(0,0,0,0.1);
          border-radius: 1rem;
          padding: 1rem;
          background: #fff;
        }
        .inline-form .form-grid { display: grid; gap: 0.6rem; }
        .inline-form input {
          width: 100%;
          padding: 0.6rem 0.8rem;
          border-radius: 0.6rem;
          border: 1px solid rgba(0,0,0,0.15);
        }
        .inline-form .form-actions { margin-top: 0.5rem; }
        .inline-form button {
          padding: 0.5rem 0.8rem;
          border-radius: 0.6rem;
          background: #000;
          color: #fff;
        }
        .result-banner { max-width: 1100px; margin: 0 auto 1rem; }
        .jobs-section .section-heading {
          display: flex; align-items: flex-end; justify-content: space-between;
          margin: 0.5rem 0 0.75rem;
        }
        .jobs-grid { display: grid; gap: 0.75rem; grid-template-columns: 1fr; }
        .empty-state {
          border: 1px dashed rgba(0,0,0,0.2);
          border-radius: 1rem;
          padding: 1rem;
          text-align: center;
        }
        .error-banner.global {
          background: #ffe9e9;
          color: #9b1c1c;
          border: 1px solid #f3b4b4;
          border-radius: 0.6rem;
          padding: 0.6rem 0.8rem;
          margin: 0.5rem 0;
        }
        @media (max-width: 980px) {
          .hero { grid-template-columns: 1fr; }
        }
      `}</style>
    </>
  );
}
