import { useState } from 'react';
import ArtifactBrowser from './ArtifactBrowser';
import { fetchManifest, fetchResultLink } from '../lib/api';
import type { TrackedJob } from '../hooks/usePersistentJobs';

interface JobCardProps {
  job: TrackedJob;
  onRefresh?: (jobId: string) => void;
  onRemove?: (jobId: string) => void;
}

export function JobCard({ job, onRefresh, onRemove }: JobCardProps) {
  const [showArtifacts, setShowArtifacts] = useState(false);
  const [showManifest, setShowManifest] = useState(false);
  const [manifest, setManifest] = useState<unknown>(null);
  const [manifestError, setManifestError] = useState<string | null>(null);
  const [manifestLoading, setManifestLoading] = useState(false);
  const [resultUrl, setResultUrl] = useState<string | null>(null);
  const [resultLoading, setResultLoading] = useState(false);
  const [resultError, setResultError] = useState<string | null>(null);

  const statusClass = job.status ? `status ${job.status.toLowerCase()}` : 'status unknown';

  const handleToggleManifest = async () => {
    const next = !showManifest;
    setShowManifest(next);
    if (!next || manifest || manifestLoading) return;

    try {
      setManifestLoading(true);
      setManifestError(null);
      const response = await fetchManifest(job.jobId);
      setManifest(response.manifest);
    } catch (error) {
      console.error('Failed to load manifest', error);
      setManifestError(error instanceof Error ? error.message : 'Unable to load manifest');
    } finally {
      setManifestLoading(false);
    }
  };

  const handleFetchResults = async () => {
    try {
      setResultLoading(true);
      setResultError(null);
      const response = await fetchResultLink(job.jobId);
      setResultUrl(response.downloadUrl);
    } catch (error) {
      console.error('Failed to fetch results', error);
      setResultError(error instanceof Error ? error.message : 'Unable to fetch results');
    } finally {
      setResultLoading(false);
    }
  };

  const created = job.createdAt ? new Date(job.createdAt * 1000) : null;
  const updated = job.updatedAt ? new Date(job.updatedAt * 1000) : null;

  return (
    <article className="job-card glass-panel">
      <header className="job-card__header">
        <div>
          <p className="eyebrow">Job</p>
          <h2>{job.jobId}</h2>
          <div className="job-meta">
            {created && <span>Created {created.toLocaleString()}</span>}
            {updated && <span>Last updated {updated.toLocaleString()}</span>}
            {job.sourceType && <span>Source: {job.sourceType}</span>}
          </div>
        </div>
        <div className="job-status-block">
          <span className={statusClass}>{job.status ?? 'Unknown'}</span>
          <div className="job-card__actions">
            <button type="button" onClick={() => onRefresh?.(job.jobId)}>
              Refresh
            </button>
            <button type="button" onClick={() => onRemove?.(job.jobId)}>
              Remove
            </button>
          </div>
        </div>
      </header>

      {job.uploadState === 'uploading' && <div className="info-banner">Uploading dataset…</div>}
      {job.uploadState === 'uploaded' && (
        <div className="success-banner subtle">Dataset uploaded. Waiting for processing…</div>
      )}
      {job.error && <div className="error-banner">{job.error}</div>}

      <section className="job-actions">
        <button type="button" onClick={() => setShowArtifacts((prev) => !prev)}>
          {showArtifacts ? 'Hide artifacts' : 'Browse artifacts'}
        </button>
        <button type="button" onClick={handleToggleManifest}>
          {showManifest ? 'Hide manifest' : 'View manifest'}
        </button>
        <button type="button" onClick={handleFetchResults} disabled={resultLoading}>
          {resultLoading ? 'Fetching…' : 'Get results link'}
        </button>
      </section>

      {resultError && <div className="error-banner">{resultError}</div>}
      {resultUrl && (
        <div className="success-banner">
          <a href={resultUrl} target="_blank" rel="noreferrer">
            Download results
          </a>
        </div>
      )}

      {showManifest && (
        <section className="manifest-viewer">
          {manifestLoading && <p className="loading">Loading manifest…</p>}
          {manifestError && <div className="error-banner">{manifestError}</div>}
          {manifest && !manifestLoading && (
            <pre>{JSON.stringify(manifest, null, 2)}</pre>
          )}
        </section>
      )}

      {showArtifacts && (
        <section className="artifact-section">
          <ArtifactBrowser jobId={job.jobId} />
        </section>
      )}
    </article>
  );
}

export default JobCard;
