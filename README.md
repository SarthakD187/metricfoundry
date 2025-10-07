# MetricFoundry

## Supported dataset ingestion

MetricFoundry's staging Lambda and LangGraph analytics pipeline can now ingest a
wide range of source formats. Upload CSV, TSV, JSON, JSONL, Excel (`.xls`,
`.xlsx`, `.xlsm`), and Parquet files directly. Database extracts produced as
SQLite databases are parsed table-by-table, with binary columns automatically
base64 encoded for compatibility. Common compression formats including GZIP,
ZIP, TAR, and TAR.GZ archives are unpacked on the fly so that nested datasets
are normalised without additional user effort. These capabilities ensure the
platform accepts virtually any structured dataset for downstream analytics.
