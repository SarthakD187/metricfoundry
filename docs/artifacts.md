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
│   ├── finalize.json
│   └── phase_payloads.zip
└── results/
    ├── correlations.csv
    ├── descriptive_stats.csv
    ├── manifest.json
    ├── outliers.json
    ├── report.html
    ├── report.txt
    ├── results.json
    └── bundles/
        ├── analytics_bundle.zip
        └── visualizations.zip
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
- `report.html` – shareable HTML narrative combining key metrics and insights.
- `report.txt` – natural-language summary for quick operator review.
- `graphs/` – PNG visualizations such as histograms and scatter plots for quick exploration of numeric fields.
- `bundles/analytics_bundle.zip` – curated download that groups descriptive
  statistics, correlation matrices, predictions, and other tabular outputs.
- `bundles/visualizations.zip` – single archive containing all generated
  histograms and scatter plots.

Additional artifacts added by future phases (plots, models, etc.) should follow
this pattern and be registered in the manifest under `artifacts/` with the
relative key `artifacts/<jobId>/<path>`.

## Phase archives

To simplify debugging, `phases/phase_payloads.zip` consolidates the individual
`<phase>.json` documents emitted by the LangGraph pipeline into a single
downloadable archive. This removes the need to fetch each phase output one at a
time when inspecting job execution details.
