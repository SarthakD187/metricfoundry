// dashboard/components/ResultsViewer.tsx

import React, { useEffect, useMemo, useState } from "react";
import type { ResultsJson } from "../lib/api";
import { listArtifacts, presign } from "../lib/api";

type Figure = { key: string; url: string };

function pretty(value: unknown) {
  try { return JSON.stringify(value, null, 2); } catch { return String(value); }
}
function formatNumber(n?: number) {
  return typeof n === "number" ? n.toLocaleString() : "—";
}

// super-naive CSV preview: split by newline + comma
async function fetchCsvPreview(url: string, maxRows = 25): Promise<{ columns: string[]; rows: string[][] }> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`CSV fetch failed (${r.status})`);
  const text = await r.text();
  const lines = text.trim().split(/\r?\n/);
  const columns = (lines.shift() ?? "").split(",");
  const rows = lines.slice(0, maxRows).map((ln) => ln.split(","));
  return { columns, rows };
}

export default function ResultsViewer({
  jobId,
  data,
  downloadUrl,
}: {
  jobId: string;
  data: ResultsJson;
  downloadUrl?: string | null;
}) {
  const rows = data?.summary?.rows;
  const cols = data?.summary?.columns;

  const [phaseFiles, setPhaseFiles] = useState<string[]>([]);
  const [figures, setFigures] = useState<Figure[]>([]);
  const [reportUrl, setReportUrl] = useState<string | null>(null);
  const [analyticsZip, setAnalyticsZip] = useState<string | null>(null);
  const [vizZip, setVizZip] = useState<string | null>(null);
  const [descCsvUrl, setDescCsvUrl] = useState<string | null>(null);
  const [corrCsvUrl, setCorrCsvUrl] = useState<string | null>(null);
  const [outliersUrl, setOutliersUrl] = useState<string | null>(null);

  const [descPreview, setDescPreview] = useState<{ columns: string[]; rows: string[][] } | null>(null);

  // Load artifacts under phases/ and results/
  useEffect(() => {
    (async () => {
      // phases
      try {
        const phases = await listArtifacts(jobId, `artifacts/${jobId}/phases/`);
        setPhaseFiles((phases.objects || []).map((o) => o.key!).filter(Boolean));
      } catch { /* ignore */ }

      // results presigns
      const presignSafe = async (key: string, setter: (v: string) => void) => {
        try { const r = await presign(jobId, key); setter(r.downloadUrl); } catch { /* ignore */ }
      };

      await presignSafe(`artifacts/${jobId}/results/report.html`, (u) => setReportUrl(u));
      await presignSafe(`artifacts/${jobId}/results/descriptive_stats.csv`, (u) => setDescCsvUrl(u));
      await presignSafe(`artifacts/${jobId}/results/correlations.csv`, (u) => setCorrCsvUrl(u));
      await presignSafe(`artifacts/${jobId}/results/outliers.json`, (u) => setOutliersUrl(u));
      await presignSafe(`artifacts/${jobId}/results/bundles/analytics_bundle.zip`, (u) => setAnalyticsZip(u));
      await presignSafe(`artifacts/${jobId}/results/bundles/visualizations.zip`, (u) => setVizZip(u));

      // figures
      try {
        const figs = await listArtifacts(jobId, `artifacts/${jobId}/results/graphs/`);
        const urls: Figure[] = [];
        for (const o of figs.objects || []) {
          if (!o.key) continue;
          const p = await presign(jobId, o.key);
          urls.push({ key: o.key, url: p.downloadUrl });
        }
        setFigures(urls);
      } catch { setFigures([]); }
    })();
  }, [jobId]);

  // CSV preview for descriptive_stats.csv
  useEffect(() => {
    (async () => {
      if (!descCsvUrl) return setDescPreview(null);
      try { setDescPreview(await fetchCsvPreview(descCsvUrl, 25)); } catch { setDescPreview(null); }
    })();
  }, [descCsvUrl]);

  const generatedAt = useMemo(
    () => (data?.generatedAt ? new Date(data.generatedAt).toLocaleString() : "—"),
    [data?.generatedAt]
  );

  return (
    <section className="results">
      <div className="kpis">
        <div className="kpi"><div className="kpi-title">Rows</div><div className="kpi-value">{formatNumber(rows)}</div></div>
        <div className="kpi"><div className="kpi-title">Columns</div><div className="kpi-value">{formatNumber(cols)}</div></div>
        <div className="kpi"><div className="kpi-title">Generated</div><div className="kpi-value">{generatedAt}</div></div>
      </div>

      <div className="cards">
        <div className="card">
          <div className="card-head">
            <h3>Result summary</h3>
            <div className="actions">
              {downloadUrl && <a className="btn" href={downloadUrl} target="_blank" rel="noreferrer">results.json</a>}
              {analyticsZip && <a className="btn" href={analyticsZip} target="_blank" rel="noreferrer">analytics bundle</a>}
              {vizZip && <a className="btn" href={vizZip} target="_blank" rel="noreferrer">visualizations</a>}
            </div>
          </div>
          <pre className="pre">{pretty({ summary: data?.summary })}</pre>
        </div>

        <div className="card">
          <div className="card-head"><h3>Descriptive statistics</h3>
            {descCsvUrl && <a className="btn" href={descCsvUrl} target="_blank" rel="noreferrer">CSV</a>}
          </div>
          {descPreview ? (
            <div className="table-wrap">
              <table className="table">
                <thead><tr>{descPreview.columns.map((c) => <th key={c}>{c}</th>)}</tr></thead>
                <tbody>
                  {descPreview.rows.map((r, i) => (
                    <tr key={i}>{r.map((cell, j) => <td key={j}>{cell}</td>)}</tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : <div className="muted">Preview unavailable.</div>}
        </div>

        <div className="card">
          <div className="card-head"><h3>Processing phases</h3></div>
          {phaseFiles.length ? (
            <ul className="list">
              {phaseFiles.map((k) => <li key={k}><code>{k}</code></li>)}
            </ul>
          ) : <div className="muted">No phase files listed.</div>}
        </div>

        {reportUrl && (
          <div className="card">
            <div className="card-head"><h3>HTML report</h3><a className="btn" href={reportUrl} target="_blank" rel="noreferrer">Open in new tab</a></div>
            <iframe src={reportUrl} className="report" sandbox="allow-same-origin allow-scripts" />
          </div>
        )}

        <div className="card">
          <div className="card-head"><h3>Correlations & Outliers</h3>
            <div className="actions">
              {corrCsvUrl && <a className="btn" href={corrCsvUrl} target="_blank" rel="noreferrer">correlations.csv</a>}
              {outliersUrl && <a className="btn" href={outliersUrl} target="_blank" rel="noreferrer">outliers.json</a>}
            </div>
          </div>
          <div className="muted">Download the diagnostics or open in your analysis tool.</div>
        </div>

        {!!figures.length && (
          <div className="card">
            <div className="card-head"><h3>Graphs</h3></div>
            <div className="fig-grid">
              {figures.map((f) => (
                <a key={f.key} href={f.url} target="_blank" rel="noreferrer" className="fig">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img src={f.url} alt={f.key} />
                </a>
              ))}
            </div>
          </div>
        )}

        <div className="card">
          <div className="card-head"><h3>Links</h3></div>
          <ul className="links">
            <li><span>Input</span><code>{data?.links?.input ?? "—"}</code></li>
            <li><span>Results manifest</span><code>{data?.links?.resultsManifest ?? "—"}</code></li>
            <li><span>Job ID</span><code>{jobId}</code></li>
          </ul>
        </div>
      </div>

      <style jsx>{`
        .results { width: 100%; max-width: 1100px; margin: 0 auto; display: flex; flex-direction: column; gap: 1rem; }
        .kpis { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 0.75rem; }
        .kpi { border: 1px solid rgba(0,0,0,0.08); border-radius: 12px; padding: 0.9rem; background: #fff; }
        .kpi-title { font-size: 0.8rem; opacity: 0.65; }
        .kpi-value { font-size: 1.25rem; font-weight: 600; }
        .cards { display: grid; grid-template-columns: 1fr; gap: 1rem; }
        .card { border: 1px solid rgba(0,0,0,0.08); border-radius: 12px; background: #fff; padding: 1rem; }
        .card-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem; gap: .5rem; flex-wrap: wrap; }
        .btn { padding: 0.45rem 0.8rem; border-radius: 10px; background: #000; color: #fff; text-decoration: none; font-size: 0.85rem; }
        .pre { white-space: pre-wrap; background: #fafafa; border-radius: 8px; padding: 0.75rem; font-size: 0.9rem; }
        .muted { opacity: 0.65; font-size: 0.9rem; }
        .list { display: grid; gap: .4rem; font-size: .92rem; }
        .report { width: 100%; height: 420px; border: 1px solid rgba(0,0,0,0.1); border-radius: 8px; }
        .fig-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: .6rem; }
        .fig { display: block; border: 1px solid rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; background: #fff; }
        .fig img { width: 100%; height: 120px; object-fit: contain; display: block; }
        .links { display: grid; grid-template-columns: 160px 1fr; gap: 0.5rem 1rem; align-items: baseline; font-size: 0.92rem; }
        .links code { word-break: break-all; }
        .table-wrap { overflow: auto; }
        .table { width: 100%; border-collapse: collapse; }
        .table th, .table td { border-bottom: 1px solid rgba(0,0,0,0.06); text-align: left; padding: 0.5rem; font-size: 0.92rem; }
        @media (max-width: 720px) { .kpis { grid-template-columns: 1fr; } .links { grid-template-columns: 1fr; } }
      `}</style>
    </section>
  );
}
