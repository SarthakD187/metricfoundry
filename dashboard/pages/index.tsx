import { FormEvent, useCallback, useEffect, useMemo, useState } from 'react';
import Head from 'next/head';
import UploadForm from '../components/UploadForm';
import JobCard from '../components/JobCard';
import { fetchJob } from '../lib/api';
import { TrackedJob, usePersistentJobs } from '../hooks/usePersistentJobs';

const POLL_INTERVAL = 5000;

export default function DashboardHome() {
  const { jobs, addJob, updateJob, removeJob } = usePersistentJobs([]);
  const [globalError, setGlobalError] = useState<string | null>(null);
  const [existingJobId, setExistingJobId] = useState('');

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
          sourceType: typeof response.source === 'object' && response.source?.type ? String(response.source.type) : undefined,
          error: (response as any).error ?? null,
        });
      } catch (error) {
        console.error('Failed to refresh job', error);
        const message = error instanceof Error ? error.message : 'Unable to refresh job';
        setGlobalError(message);
      }
    },
    [updateJob],
  );

  const handleJobCreated = (jobId: string, metadata: { sourceType: string }) => {
    const initial: TrackedJob = {
      jobId,
      status: metadata.sourceType === 'upload' ? 'CREATED' : 'QUEUED',
      sourceType: metadata.sourceType,
      uploadState: metadata.sourceType === 'upload' ? 'uploading' : 'idle',
    };
    addJob(initial);
    refreshJob(jobId);
  };

  const handleUploadFinished = (jobId: string) => {
    updateJob(jobId, { uploadState: 'uploaded' });
    refreshJob(jobId);
  };

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
        sourceType: typeof response.source === 'object' && response.source?.type ? String(response.source.type) : undefined,
      });
      setExistingJobId('');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to find job';
      setGlobalError(message);
    }
  };

  const activeJobIds = useMemo(() =>
    jobs
      .filter((job) => {
        const status = job.status?.toUpperCase?.() ?? 'UNKNOWN';
        return !['SUCCEEDED', 'FAILED'].includes(status);
      })
      .map((job) => job.jobId),
  [jobs]);

  useEffect(() => {
    if (activeJobIds.length === 0) return undefined;
    const interval = setInterval(() => {
      activeJobIds.forEach((jobId) => {
        refreshJob(jobId);
      });
    }, POLL_INTERVAL);
    return () => clearInterval(interval);
  }, [activeJobIds.join(','), refreshJob]);

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
              Upload datasets, orchestrate ingestion jobs, and explore artifacts with a modern, responsive interface
              backed by the MetricFoundry API.
            </p>
          </div>
          <div className="hero-panels">
            <UploadForm
              onJobCreated={handleJobCreated}
              onUploadFinished={handleUploadFinished}
              onError={(message) => setGlobalError(message)}
            />
            <form className="glass-panel inline-form" onSubmit={handleExistingJobSubmit}>
              <p className="eyebrow">Track an existing job</p>
              <h2>Already have a job ID?</h2>
              <p className="subtitle">Paste it below to start monitoring progress and outputs instantly.</p>
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

        <section className="jobs-section">
          <div className="section-heading">
            <div>
              <p className="eyebrow">Jobs</p>
              <h2>Recent activity</h2>
            </div>
            <p className="subtitle">
              Jobs are stored locally in your browser for convenience. Finalised jobs can be removed from the list without
              affecting the underlying data.
            </p>
          </div>

          {jobs.length === 0 ? (
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
    </>
  );
}
