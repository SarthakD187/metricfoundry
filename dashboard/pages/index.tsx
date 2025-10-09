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

  const latestUpdate = useMemo(() => {
    const timestamps = jobs
      .map((job) => job.updatedAt ?? job.createdAt ?? null)
      .filter((value): value is number => typeof value === "number" && value > 0);
    if (timestamps.length === 0) return null;
    return new Date(Math.max(...timestamps) * 1000);
  }, [jobs]);

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
      <main className="shell">
        <span className="aurora aurora-one" aria-hidden />
        <span className="aurora aurora-two" aria-hidden />
        <header className="masthead">
          <div className="brand">
            <span className="brand-mark" />
            <span className="brand-text">MetricFoundry</span>
          </div>
          <div className="masthead-copy">
            <p>Realtime visibility for every ingestion and analysis job.</p>
            <div className="masthead-glint" aria-hidden />
          </div>
        </header>

        <section className="hero-block">
          <div className="hero-copy">
            <h1>
              Shape data runs
              <br />
              with cinematic clarity
            </h1>
            <p>
              Launch fresh uploads, monitor pipelines, and unlock shareable insights in a single immersive command
              center tailored for modern data teams.
            </p>
            <div className="hero-metrics">
              <div className="metric">
                <span className="metric-label">Active jobs</span>
                <span className="metric-value">{activeJobIds.length}</span>
              </div>
              <div className="metric">
                <span className="metric-label">Tracked</span>
                <span className="metric-value">{jobs.length}</span>
              </div>
              <div className="metric">
                <span className="metric-label">Last update</span>
                <span className="metric-value">{mounted && latestUpdate ? latestUpdate.toLocaleTimeString() : "—"}</span>
              </div>
            </div>
          </div>

          <div className="hero-actions">
            <UploadForm
              onJobCreated={async () => {
                const { jobId, uploadUrl } = await handleJobCreated();
                return {
                  jobId,
                  upload: async (file: File | Blob) => {
                    await uploadToPresigned(uploadUrl, file);
                    await handleUpload(jobId, file);
                    await handleUploadFinished(jobId, uploadUrl);
                  },
                };
              }}
              onError={(message) => setGlobalError(message)}
            />

            <form className="panel existing-job" onSubmit={handleExistingJobSubmit}>
              <div className="panel-heading">
                <p className="panel-label">Follow a job</p>
                <h2>Reconnect to an existing run</h2>
              </div>
              <p className="panel-description">
                Drop in a job identifier to resume monitoring and retrieve the freshest artifacts instantly.
              </p>
              <label className="field">
                <span>Job ID</span>
                <input
                  type="text"
                  value={existingJobId}
                  onChange={(event) => setExistingJobId(event.target.value)}
                  placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                />
              </label>
              <div className="form-actions">
                <button type="submit" className="action">
                  Add job
                </button>
              </div>
            </form>
          </div>
        </section>

        {globalError && <div className="toast toast-error">{globalError}</div>}

        {activeResult && (
          <section className="results-stage">
            <div className="panel spotlight">
              <div className="panel-heading">
                <p className="panel-label">Latest analysis</p>
                <h2>Job {activeResult.jobId}</h2>
              </div>
              <ResultsViewer jobId={activeResult.jobId} data={activeResult.json} downloadUrl={activeResult.downloadUrl} />
            </div>
          </section>
        )}

        <section className="jobs-board">
          <div className="board-heading">
            <div>
              <p className="panel-label">Job timeline</p>
              <h2>Recent activity</h2>
            </div>
            <p className="board-copy">
              Job metadata lives locally for instant recall. Clean up entries here without touching the remote pipeline.
            </p>
          </div>

          {!mounted ? (
            <div className="empty-panel">
              <h3>Preparing your workspace</h3>
              <p>Hang tight while we hydrate your stored jobs.</p>
            </div>
          ) : jobs.length === 0 ? (
            <div className="empty-panel">
              <h3>No runs yet</h3>
              <p>Launch a dataset upload above to see it stream in here.</p>
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
        .shell {
          position: relative;
          min-height: 100vh;
          padding: clamp(2rem, 4vw, 4rem);
          display: flex;
          flex-direction: column;
          gap: clamp(2.5rem, 5vw, 4.5rem);
        }
        .aurora {
          position: fixed;
          border-radius: 999px;
          filter: blur(90px);
          opacity: 0.6;
          animation: float 14s ease-in-out infinite;
          pointer-events: none;
        }
        .aurora-one {
          width: 420px;
          height: 420px;
          background: radial-gradient(circle, rgba(136, 73, 143, 0.65), rgba(86, 65, 84, 0));
          top: -120px;
          right: -160px;
          animation-delay: -4s;
        }
        .aurora-two {
          width: 380px;
          height: 380px;
          background: radial-gradient(circle, rgba(255, 101, 66, 0.5), rgba(136, 73, 143, 0));
          bottom: -140px;
          left: -120px;
        }
        @keyframes float {
          0%,
          100% {
            transform: translate3d(0, 0, 0);
          }
          50% {
            transform: translate3d(12px, -14px, 0) scale(1.04);
          }
        }
        .masthead {
          display: flex;
          flex-direction: column;
          gap: 0.75rem;
          position: relative;
        }
        .brand {
          display: inline-flex;
          align-items: center;
          gap: 0.65rem;
          font-weight: 700;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: #e0cba8;
        }
        .brand-mark {
          width: 32px;
          height: 32px;
          border-radius: 10px;
          background: linear-gradient(135deg, rgba(255, 101, 66, 0.8), rgba(136, 73, 143, 0.8));
          box-shadow: 0 0 0 1px rgba(224, 203, 168, 0.4);
        }
        .brand-text {
          font-size: 0.85rem;
        }
        .masthead-copy {
          display: flex;
          align-items: center;
          gap: 1.5rem;
          color: rgba(224, 203, 168, 0.75);
          font-size: 0.95rem;
        }
        .masthead-glint {
          flex: 1;
          height: 2px;
          background: linear-gradient(
            90deg,
            rgba(224, 203, 168, 0),
            rgba(224, 203, 168, 0.9),
            rgba(224, 203, 168, 0)
          );
          animation: shimmer 6s linear infinite;
        }
        @keyframes shimmer {
          0% {
            opacity: 0.2;
          }
          50% {
            opacity: 1;
          }
          100% {
            opacity: 0.2;
          }
        }
        .hero-block {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
          gap: clamp(1.75rem, 3vw, 2.5rem);
        }
        .hero-copy {
          display: grid;
          gap: 1.4rem;
        }
        .hero-copy h1 {
          margin: 0;
          font-size: clamp(2.6rem, 4vw, 3.6rem);
          line-height: 1.05;
          color: #ff6542;
          text-shadow: 0 6px 22px rgba(255, 101, 66, 0.35);
        }
        .hero-copy p {
          margin: 0;
          font-size: 1.05rem;
          color: rgba(224, 203, 168, 0.78);
        }
        .hero-metrics {
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 0.75rem;
        }
        .metric {
          padding: 1rem 1.1rem;
          border-radius: 20px;
          background: rgba(119, 159, 161, 0.18);
          border: 1px solid rgba(224, 203, 168, 0.28);
          display: grid;
          gap: 0.4rem;
          box-shadow: 0 16px 32px -24px rgba(86, 65, 84, 0.55);
        }
        .metric-label {
          font-size: 0.75rem;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: rgba(224, 203, 168, 0.6);
        }
        .metric-value {
          font-size: 1.45rem;
          font-weight: 700;
          color: #e0cba8;
        }
        .hero-actions {
          display: grid;
          gap: 1.5rem;
        }
        .panel {
          position: relative;
          border-radius: 28px;
          padding: clamp(1.6rem, 3vw, 2.2rem);
          background: linear-gradient(145deg, rgba(86, 65, 84, 0.66), rgba(119, 159, 161, 0.18));
          border: 1px solid rgba(224, 203, 168, 0.25);
          box-shadow: 0 30px 60px -40px rgba(86, 65, 84, 0.7);
          backdrop-filter: blur(14px);
        }
        .panel::after {
          content: "";
          position: absolute;
          inset: 0;
          border-radius: inherit;
          border: 1px solid rgba(255, 101, 66, 0.12);
          mask: linear-gradient(180deg, rgba(86, 65, 84, 0.3), transparent 60%);
          pointer-events: none;
        }
        .panel-heading {
          display: grid;
          gap: 0.4rem;
          margin-bottom: 0.75rem;
        }
        .panel-label {
          margin: 0;
          font-size: 0.75rem;
          font-weight: 700;
          letter-spacing: 0.12em;
          text-transform: uppercase;
          color: rgba(224, 203, 168, 0.62);
        }
        .panel-heading h2 {
          margin: 0;
          font-size: 1.6rem;
          color: #e0cba8;
        }
        .panel-description {
          margin: 0 0 1rem;
          color: rgba(224, 203, 168, 0.68);
          font-size: 0.95rem;
        }
        .existing-job .field {
          display: grid;
          gap: 0.5rem;
          color: rgba(224, 203, 168, 0.75);
          font-weight: 600;
        }
        .existing-job input {
          border-radius: 16px;
          padding: 0.75rem 1rem;
          border: 1px solid rgba(224, 203, 168, 0.35);
          background: rgba(86, 65, 84, 0.65);
          color: #e0cba8;
        }
        .existing-job input::placeholder {
          color: rgba(224, 203, 168, 0.4);
        }
        .existing-job input:focus {
          outline: 2px solid rgba(255, 101, 66, 0.5);
          outline-offset: 2px;
        }
        .form-actions {
          margin-top: 1.25rem;
        }
        .action {
          border: none;
          border-radius: 999px;
          padding: 0.75rem 1.6rem;
          font-size: 0.95rem;
          font-weight: 700;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: #564154;
          background: linear-gradient(135deg, rgba(255, 101, 66, 0.92), rgba(224, 203, 168, 0.92));
          box-shadow: 0 22px 40px -30px rgba(255, 101, 66, 0.8);
          cursor: pointer;
          transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        .action:hover,
        .action:focus-visible {
          transform: translateY(-2px);
          box-shadow: 0 26px 60px -24px rgba(255, 101, 66, 0.85);
        }
        .toast {
          align-self: center;
          padding: 0.75rem 1.5rem;
          border-radius: 999px;
          border: 1px solid rgba(255, 101, 66, 0.45);
          color: #ff6542;
          background: rgba(255, 101, 66, 0.12);
          font-weight: 600;
        }
        .results-stage {
          display: flex;
          justify-content: center;
        }
        .spotlight {
          width: min(100%, 1120px);
        }
        .jobs-board {
          display: grid;
          gap: 1.75rem;
        }
        .board-heading {
          display: flex;
          flex-direction: column;
          gap: 0.75rem;
        }
        .board-heading h2 {
          margin: 0;
          font-size: 2rem;
          color: #e0cba8;
        }
        .board-copy {
          margin: 0;
          color: rgba(224, 203, 168, 0.68);
          max-width: 520px;
        }
        .empty-panel {
          border-radius: 26px;
          padding: 2.5rem;
          border: 1px dashed rgba(224, 203, 168, 0.45);
          background: rgba(119, 159, 161, 0.12);
          text-align: center;
          color: rgba(224, 203, 168, 0.75);
          display: grid;
          gap: 0.6rem;
        }
        .empty-panel h3 {
          margin: 0;
          color: #e0cba8;
        }
        .empty-panel p {
          margin: 0;
        }
        .jobs-grid {
          display: grid;
          gap: 1.5rem;
        }
        @media (max-width: 720px) {
          .hero-metrics {
            grid-template-columns: 1fr;
          }
        }
      `}</style>
    </>
  );
}
