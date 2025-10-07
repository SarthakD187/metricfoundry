# Analytics Artifact Layout

The analytics processor and LangGraph worker collaborate to publish a predictable
set of artifacts for every job. For a job with identifier `<jobId>` the
following directory layout is produced within the configured artifacts bucket:

```
artifacts/<jobId>/
├── phases/
│   ├── ingest.json
│   ├── profile.json
│   ├── dq_validate.json
│   ├── descriptive_stats.json
│   ├── nl_report.json
│   └── finalize.json
└── results/
    ├── correlations.csv
    ├── descriptive_stats.csv
    ├── manifest.json
    ├── outliers.json
    ├── report.txt
    └── results.json
```

## Phase artifacts

Each LangGraph node emits a JSON document that is uploaded to the corresponding
`phases/<phase>.json` key. These files contain the complete payload returned by
that phase and mirror the values that appear under `phases` in `results.json`.

## Result artifacts

The `results/` directory aggregates outputs that are useful across multiple
phases:

- `results.json` – consolidated analytics payload consumed by the API.
- `manifest.json` – enumerates every downloadable asset, including content type
  and a short description.
- `descriptive_stats.csv` – tabular descriptive statistics for numeric columns.
- `correlations.csv` – pairwise Pearson correlations derived from numeric
  samples.
- `outliers.json` – detected outliers for numeric fields along with z-scores.
- `report.txt` – natural-language summary for quick operator review.
- `graphs/` – PNG visualizations such as histograms and scatter plots for quick exploration of numeric fields.

Additional artifacts added by future phases (plots, models, etc.) should follow
this pattern and be registered in the manifest under `artifacts/` with the
relative key `artifacts/<jobId>/<path>`.
