import { useEffect, useMemo, useState } from 'react';
import { ArtifactListResponse, ArtifactObjectSummary, listArtifacts } from '../lib/api';

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
        const response: ArtifactListResponse = await listArtifacts(jobId, {
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

  const breadcrumbs = useMemo(() => {
    const parts = prefix
      .replace(basePrefix, '')
      .split('/')
      .filter(Boolean);

    const crumbs = parts.map((segment, index) => {
      const target = `${basePrefix}${parts.slice(0, index + 1).join('/')}/`;
      return { label: segment, target };
    });

    return [
      { label: 'Job root', target: basePrefix, isActive: crumbs.length === 0 },
      ...crumbs.map((crumb, index) => ({
        ...crumb,
        isActive: index === crumbs.length - 1,
      })),
    ];
  }, [basePrefix, prefix]);

  const handleLoadMore = () => {
    if (!nextToken || loading) return;
    setLoading(true);
    listArtifacts(jobId, { prefix, continuationToken: nextToken, pageSize: PAGE_SIZE })
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
      })
      .finally(() => {
        setLoading(false);
      });
  };

  return (
    <div className="artifact-browser">
      <section className="artifact-shell">
        <header className="artifact-shell__header">
          <div>
            <span className="artifact-chip">Artifacts</span>
            <h3>{relativePrefix === '/' ? 'All outputs' : relativePrefix}</h3>
          </div>
          <div className="artifact-shell__controls">
            {parent && (
              <button type="button" className="artifact-button ghost" onClick={() => setPrefix(parent)}>
                ← Parent folder
              </button>
            )}
          </div>
        </header>

        <nav className="artifact-breadcrumbs" aria-label="Artifact breadcrumbs">
          {breadcrumbs.map((crumb, index) => (
            <button
              key={`${crumb.label}-${index}`}
              type="button"
              className={`artifact-breadcrumb ${crumb.isActive ? 'active' : ''}`}
              onClick={() => !crumb.isActive && setPrefix(crumb.target)}
              disabled={crumb.isActive}
            >
              {crumb.label}
            </button>
          ))}
        </nav>

        {error && <div className="artifact-banner error">{error}</div>}

        <div className="artifact-tiles" role="list">
          {objects.length === 0 && !loading && (
            <div className="artifact-empty" role="note">
              <h4>No artifacts yet</h4>
              <p>Generated files for this job will appear here once the run completes.</p>
            </div>
          )}

          {objects.map((entry) => {
            const key = `${entry.type}-${entry.type === 'folder' ? entry.prefix : entry.key}`;
            const metaText =
              entry.type === 'folder'
                ? 'Folder'
                : `${entry.lastModified ? new Date(entry.lastModified).toLocaleString() : 'Unknown date'} · ${formatBytes(entry.size)}`;

            return (
              <div key={key} className={`artifact-tile ${entry.type}`} role="listitem">
                <div className="artifact-tile__body">
                  <div className="artifact-icon" aria-hidden>
                    {entry.type === 'folder' ? '📁' : '📄'}
                  </div>
                  <div className="artifact-tile__text">
                    <span className="artifact-tile__name">
                      {entry.name || (entry.type === 'folder' ? '(folder)' : '(file)')}
                    </span>
                    <span className="artifact-tile__meta">{metaText}</span>
                  </div>
                </div>
                <div className="artifact-tile__actions">
                  {entry.type === 'folder' ? (
                    <button type="button" className="artifact-button" onClick={() => setPrefix(entry.prefix!)}>
                      Open folder
                    </button>
                  ) : (
                    <button
                      type="button"
                      className="artifact-button ghost"
                      onClick={() => navigator.clipboard?.writeText(entry.key ?? '')}
                      title="Copy object key"
                    >
                      Copy key
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {loading && (
          <div className="artifact-loader" role="status" aria-live="polite">
            <span className="artifact-loader__dot" aria-hidden />
            <span>{objects.length ? 'Loading more items…' : 'Loading artifacts…'}</span>
          </div>
        )}

        {nextToken && (
          <div className="artifact-shell__footer">
            <button type="button" className="artifact-button" onClick={handleLoadMore} disabled={loading}>
              {loading ? 'Loading…' : 'Load more'}
            </button>
          </div>
        )}
      </section>
    </div>
  );
}

export default ArtifactBrowser;
