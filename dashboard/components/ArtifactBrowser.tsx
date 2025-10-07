import { useEffect, useMemo, useState } from 'react';
import { ArtifactListResponse, ArtifactObjectSummary, fetchArtifacts } from '../lib/api';

interface ArtifactBrowserProps {
  jobId: string;
}

interface ArtifactEntry extends ArtifactObjectSummary {
  type: 'file' | 'folder';
  name: string;
  prefix?: string;
}

const PAGE_SIZE = 50;

function formatBytes(size?: number): string {
  if (!size && size !== 0) return '—';
  if (size === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const exponent = Math.min(units.length - 1, Math.floor(Math.log(size) / Math.log(1024)));
  const value = size / 1024 ** exponent;
  return `${value.toFixed(value < 10 && exponent > 0 ? 1 : 0)} ${units[exponent]}`;
}

function parentPrefix(prefix: string, base: string): string | null {
  if (prefix === base) return null;
  const trimmed = prefix.endsWith('/') ? prefix.slice(0, -1) : prefix;
  const withoutBase = trimmed.replace(base, '');
  const segments = withoutBase.split('/').filter(Boolean);
  if (segments.length <= 1) return base;
  segments.pop();
  return `${base}${segments.join('/')}/`;
}

export function ArtifactBrowser({ jobId }: ArtifactBrowserProps) {
  const basePrefix = useMemo(() => `artifacts/${jobId}/`, [jobId]);
  const [prefix, setPrefix] = useState(basePrefix);
  const [objects, setObjects] = useState<ArtifactEntry[]>([]);
  const [nextToken, setNextToken] = useState<string | null | undefined>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setPrefix(basePrefix);
  }, [basePrefix]);

  useEffect(() => {
    let isCancelled = false;
    async function loadArtifacts(target: string, token?: string | null, append = false) {
      try {
        setLoading(true);
        setError(null);
        const response: ArtifactListResponse = await fetchArtifacts(jobId, {
          prefix: target,
          continuationToken: token ?? undefined,
          pageSize: PAGE_SIZE,
        });

        if (isCancelled) return;
        const entries: ArtifactEntry[] = [];
        const directories = (response.commonPrefixes ?? [])
          .filter((item): item is string => Boolean(item))
          .map((item) => ({
            type: 'folder' as const,
            name: item!.replace(target, '').replace(/\/$/, ''),
            prefix: item!,
          }));
        const files = (response.objects ?? []).map((obj) => ({
          ...obj,
          type: 'file' as const,
          name: obj.key ? obj.key.replace(target, '') : 'unnamed',
        }));

        const combined = [...directories, ...files];
        setObjects((prev) => (append ? [...prev, ...combined] : combined));
        setNextToken(response.nextToken ?? null);
      } catch (err) {
        if (isCancelled) return;
        console.error('Failed to load artifacts', err);
        setError(err instanceof Error ? err.message : 'Unable to load artifacts');
      } finally {
        if (!isCancelled) {
          setLoading(false);
        }
      }
    }

    loadArtifacts(prefix);
    return () => {
      isCancelled = true;
    };
  }, [jobId, prefix]);

  const relativePrefix = prefix.replace(basePrefix, '') || '/';
  const parent = parentPrefix(prefix, basePrefix);

  const handleLoadMore = () => {
    if (!nextToken) return;
    fetchArtifacts(jobId, { prefix, continuationToken: nextToken, pageSize: PAGE_SIZE })
      .then((response) => {
        const directories = (response.commonPrefixes ?? [])
          .filter((item): item is string => Boolean(item))
          .map((item) => ({
            type: 'folder' as const,
            name: item!.replace(prefix, '').replace(/\/$/, ''),
            prefix: item!,
          }));
        const files = (response.objects ?? []).map((obj) => ({
          ...obj,
          type: 'file' as const,
          name: obj.key ? obj.key.replace(prefix, '') : 'unnamed',
        }));
        setObjects((prev) => [...prev, ...directories, ...files]);
        setNextToken(response.nextToken ?? null);
      })
      .catch((err) => {
        console.error('Failed to load more artifacts', err);
        setError(err instanceof Error ? err.message : 'Unable to load more artifacts');
      });
  };

  return (
    <div className="artifact-browser">
      <div className="artifact-header">
        <div>
          <p className="eyebrow">Artifacts</p>
          <h3>{relativePrefix}</h3>
        </div>
        <div className="artifact-actions">
          {parent && (
            <button type="button" onClick={() => setPrefix(parent)}>
              ← Up a level
            </button>
          )}
        </div>
      </div>

      {error && <div className="error-banner">{error}</div>}

      <div className="artifact-table" role="list">
        {objects.length === 0 && !loading && <p className="empty-state">No artifacts yet.</p>}

        {objects.map((entry) => (
          <div
            key={`${entry.type}-${entry.type === 'folder' ? entry.prefix : entry.key}`}
            className={`artifact-row ${entry.type}`}
            role="listitem"
          >
            <div className="artifact-meta">
              <span className="artifact-name">{entry.name || (entry.type === 'folder' ? '(folder)' : '(file)')}</span>
              <span className="artifact-subtle">
                {entry.type === 'folder'
                  ? 'Folder'
                  : `${entry.lastModified ? new Date(entry.lastModified).toLocaleString() : 'Unknown date'} · ${formatBytes(entry.size)}`}
              </span>
            </div>
            <div className="artifact-actions">
              {entry.type === 'folder' ? (
                <button type="button" onClick={() => setPrefix(entry.prefix!)}>
                  Open
                </button>
              ) : (
                <button
                  type="button"
                  onClick={() => navigator.clipboard?.writeText(entry.key ?? '')}
                  title="Copy object key"
                >
                  Copy key
                </button>
              )}
            </div>
          </div>
        ))}
      </div>

      {nextToken && (
        <div className="load-more">
          <button type="button" onClick={handleLoadMore} disabled={loading}>
            {loading ? 'Loading…' : 'Load more'}
          </button>
        </div>
      )}
    </div>
  );
}

export default ArtifactBrowser;
