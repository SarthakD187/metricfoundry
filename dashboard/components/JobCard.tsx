import { useState } from 'react';
import ArtifactBrowser from './ArtifactBrowser';
import ManifestPreview from './ManifestPreview';
import { fetchManifest, fetchResultLink, ManifestPayload } from '../lib/api';
import type { TrackedJob } from '../hooks/usePersistentJobs';

interface JobCardProps {
  job: TrackedJob;
  onRefresh?: (jobId: string) => void;
  onRemove?: (jobId: string) => void;
}

export function JobCard({ job, onRefresh, onRemove }: JobCardProps) {
  const [showArtifacts, setShowArtifacts] = useState(false);
  const [showManifest, setShowManifest] = useState(false);
  const [manifest, setManifest] = useState<ManifestPayload | null>(null);
  const [manifestError, setManifestError] = useState<string | null>(null);
  const [manifestLoading, setManifestLoading] = useState(false);
  const [resultUrl, setResultUrl] = useState<string | null>(null);
  const [resultLoading, setResultLoading] = useState(false);
  const [resultError, setResultError] = useState<string | null>(null);

  const statusClass = job.status ? `status-pill ${job.status.toLowerCase()}` : 'status-pill unknown';

  const handleToggleManifest = async () => {
    const next = !showManifest;
    setShowManifest(next);
    if (!next || manifest || manifestLoading) return;

    try {
      setManifestLoading(true);
      setManifestError(null);
      const response = await fetchManifest(job.jobId);
      setManifest(response.manifest ?? null);
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
    <article className="panel job-card">
      <header className="job-card__header">
        <div className="job-card__title">
          <span className="job-chip">{job.sourceType ?? 'Job'}</span>
          <h3>{job.jobId}</h3>
          <div className="job-meta">
            {created && <span>Created {created.toLocaleString()}</span>}
            {updated && <span>Updated {updated.toLocaleString()}</span>}
          </div>
        </div>
        <div className="job-status-block">
          <span className={statusClass}>{job.status ?? 'Unknown'}</span>
          <div className="job-card__actions">
            <button type="button" className="tertiary" onClick={() => onRefresh?.(job.jobId)}>
              Refresh
            </button>
            <button type="button" className="tertiary" onClick={() => onRemove?.(job.jobId)}>
              Remove
            </button>
          </div>
        </div>
      </header>

      {job.uploadState === 'uploading' && <div className="banner info">Uploading dataset…</div>}
      {job.uploadState === 'uploaded' && (
        <div className="banner success">Dataset uploaded. Waiting for processing…</div>
      )}
      {job.error && <div className="banner danger">{job.error}</div>}

      <section className="job-actions">
        <button type="button" className="primary" onClick={() => setShowArtifacts((prev) => !prev)}>
          {showArtifacts ? 'Hide artifacts' : 'Browse artifacts'}
        </button>
        <button type="button" className="primary" onClick={handleToggleManifest}>
          {showManifest ? 'Hide manifest' : 'View manifest'}
        </button>
        <button type="button" className="primary" onClick={handleFetchResults} disabled={resultLoading}>
          {resultLoading ? 'Fetching…' : 'Get results link'}
        </button>
      </section>

      {resultError && <div className="banner danger">{resultError}</div>}
      {resultUrl && (
        <div className="banner success">
          <a href={resultUrl} target="_blank" rel="noreferrer">
            Download results
          </a>
        </div>
      )}

      {showManifest && (
        <section className="manifest-viewer">
          {manifestLoading && <p className="loading">Loading manifest…</p>}
          {manifestError && <div className="banner danger">{manifestError}</div>}
          {manifest && !manifestLoading && (
            <ManifestPreview jobId={job.jobId} manifest={manifest} />
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
