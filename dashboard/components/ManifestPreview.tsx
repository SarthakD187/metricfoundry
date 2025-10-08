import { useState } from 'react';
import { fetchResultLink, ManifestArtifactEntry, ManifestPayload } from '../lib/api';

interface ManifestPreviewProps {
  jobId: string;
  manifest: ManifestPayload;
}

interface ArtifactPreviewState {
  kind: 'table' | 'image' | 'text' | 'json' | 'unsupported';
  headers?: string[];
  rows?: string[][];
  src?: string;
  text?: string;
  reason?: string;
}

interface ArtifactPreviewCardProps {
  jobId: string;
  artifact: ManifestArtifactEntry;
  basePath?: string;
}

function normalizeBasePath(jobId: string, basePath?: string): string {
  if (basePath && basePath.startsWith('artifacts/')) {
    return basePath.endsWith('/') ? basePath : `${basePath}/`;
  }
  return `artifacts/${jobId}/`;
}

function resultsRelativePath(jobId: string, artifact: ManifestArtifactEntry, basePath?: string): string | null {
  if (!artifact.key) return null;
  const base = normalizeBasePath(jobId, basePath);
  if (!artifact.key.startsWith(base)) return null;
  const relative = artifact.key.slice(base.length);
  return relative.startsWith('results/') ? relative : null;
}

function truncateText(value: string, limit = 1000): string {
  if (value.length <= limit) return value;
  return `${value.slice(0, limit)}…`;
}

function stripHtml(source: string): string {
  return source.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function parseCsv(content: string, limit = 6): string[][] {
  const lines = content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const rows: string[][] = [];
  for (const line of lines) {
    const parsed: string[] = [];
    let field = '';
    let inQuotes = false;
    for (let index = 0; index < line.length; index += 1) {
      const char = line[index];
      if (char === '"') {
        if (inQuotes && line[index + 1] === '"') {
          field += '"';
          index += 1;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        parsed.push(field);
        field = '';
      } else {
        field += char;
      }
    }
    parsed.push(field);
    rows.push(parsed.map((value) => value.trim()));
    if (rows.length >= limit) break;
  }
  return rows;
}

async function loadArtifactPreview(
  jobId: string,
  artifact: ManifestArtifactEntry,
  basePath?: string,
): Promise<ArtifactPreviewState> {
  const contentType = (artifact.contentType ?? '').toLowerCase();
  const relativePath = resultsRelativePath(jobId, artifact, basePath);
  if (!relativePath) {
    return { kind: 'unsupported', reason: 'Preview available for results artifacts only.' };
  }

  if (!contentType || /zip|octet-stream/.test(contentType)) {
    return { kind: 'unsupported', reason: 'Preview not supported for this artifact type.' };
  }

  const { downloadUrl } = await fetchResultLink(jobId, relativePath);

  if (contentType.startsWith('image/')) {
    return { kind: 'image', src: downloadUrl };
  }

  const response = await fetch(downloadUrl, {
    headers: { Range: 'bytes=0-65535' },
  });
  if (!response.ok) {
    throw new Error('Unable to load artifact preview');
  }
  const text = await response.text();

  if (contentType.includes('csv')) {
    const rows = parseCsv(text);
    if (rows.length === 0) {
      return { kind: 'unsupported', reason: 'No rows available for preview.' };
    }
    const [header, ...data] = rows;
    return { kind: 'table', headers: header, rows: data };
  }

  if (contentType.includes('json')) {
    try {
      const parsed = JSON.parse(text);
      const formatted = JSON.stringify(parsed, null, 2);
      return { kind: 'json', text: truncateText(formatted, 2000) };
    } catch (error) {
      return { kind: 'json', text: truncateText(text, 2000) };
    }
  }

  if (contentType.includes('html')) {
    return { kind: 'text', text: truncateText(stripHtml(text)) };
  }

  if (contentType.startsWith('text/')) {
    return { kind: 'text', text: truncateText(text) };
  }

  return { kind: 'unsupported', reason: 'Preview not supported for this artifact type.' };
}

function ArtifactPreviewCard({ jobId, artifact, basePath }: ArtifactPreviewCardProps) {
  const [expanded, setExpanded] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<ArtifactPreviewState | null>(null);

  const handleToggle = async () => {
    const next = !expanded;
    setExpanded(next);
    if (!next || preview || loading) return;
    try {
      setLoading(true);
      setError(null);
      const loaded = await loadArtifactPreview(jobId, artifact, basePath);
      setPreview(loaded);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unable to load preview');
    } finally {
      setLoading(false);
    }
  };

  const title = artifact.name || artifact.key?.split('/').pop() || 'Artifact';
  const description = artifact.description || 'Generated artifact';
  const relative = resultsRelativePath(jobId, artifact, basePath);

  return (
    <article className="artifact-preview-card">
      <header className="artifact-preview-card__header">
        <div>
          <h4>{title}</h4>
          <p className="artifact-preview-description">{description}</p>
          {relative && <p className="artifact-preview-path">{relative}</p>}
        </div>
        <div className="artifact-preview-actions">
          <span className="artifact-preview-meta">{artifact.contentType ?? 'Unknown type'}</span>
          <button type="button" onClick={handleToggle}>
            {expanded ? 'Hide preview' : 'Show preview'}
          </button>
        </div>
      </header>
      {expanded && (
        <div className="artifact-preview-body">
          {loading && <p className="loading">Loading preview…</p>}
          {error && <div className="error-banner">{error}</div>}
          {preview && !loading && !error && (
            <ArtifactPreviewContent preview={preview} title={title} />
          )}
        </div>
      )}
    </article>
  );
}

function ArtifactPreviewContent({ preview, title }: { preview: ArtifactPreviewState; title: string }) {
  switch (preview.kind) {
    case 'image':
      return <img className="artifact-preview-image" src={preview.src ?? ''} alt={title} />;
    case 'table':
      return (
        <table className="artifact-preview-table">
          <thead>
            <tr>
              {(preview.headers ?? []).map((header) => (
                <th key={header}>{header || '—'}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(preview.rows ?? []).map((row, rowIndex) => (
              <tr key={`${title}-row-${rowIndex}`}>
                {row.map((cell, cellIndex) => (
                  <td key={`${title}-cell-${rowIndex}-${cellIndex}`}>{cell || '—'}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      );
    case 'json':
      return <pre className="artifact-preview-text">{preview.text}</pre>;
    case 'text':
      return <pre className="artifact-preview-text">{preview.text}</pre>;
    case 'unsupported':
    default:
      return <p className="artifact-preview-unsupported">{preview.reason ?? 'Preview unavailable.'}</p>;
  }
}

export function ManifestPreview({ jobId, manifest }: ManifestPreviewProps) {
  const artifacts = manifest.artifacts ?? [];
  const basePath = manifest.basePath;

  return (
    <div className="manifest-preview">
      <div className="manifest-summary">
        <div>
          <p className="eyebrow">Artifact manifest</p>
          <h3>{artifacts.length ? `${artifacts.length} artifacts` : 'No artifacts yet'}</h3>
        </div>
        {manifest.basePath && <span className="artifact-preview-meta">Base path: {manifest.basePath}</span>}
      </div>
      <div className="artifact-preview-list">
        {artifacts.length === 0 && <p className="empty-state">Artifacts will appear here once processing completes.</p>}
        {artifacts.map((artifact) => (
          <ArtifactPreviewCard
            key={artifact.key ?? artifact.name}
            jobId={jobId}
            artifact={artifact}
            basePath={basePath}
          />
        ))}
      </div>
    </div>
  );
}

export default ManifestPreview;
