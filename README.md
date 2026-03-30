# Media Metadata Explorer

## Overview

Media Metadata Explorer is a local-first tool designed to bring structure to large, unorganized collections of photos and videos. It scans directories, extracts metadata (EXIF, GPS, device info, timestamps), and stores it in a queryable database.

The goal is simple: make your media collection searchable, explorable, and actually usable.

---

## Phase 1 (Current)

Local-first ingestion and metadata extraction for large personal media collections.

### What this implements

* recursive scan of a root folder
* supported image type detection
* filesystem metadata collection
* embedded metadata extraction via ExifTool
* basic filename datetime parsing
* normalization into a stable flat schema
* persistence in DuckDB
* export to CSV or Parquet
* coverage and failure summary reporting

---

## Features (Planned)

### Phase 2 – Exploration UI

* Filterable table view
* Photo gallery ("Show Photos")
* Timeline visualization
* Map view (GPS-based)

### Phase 3 – Intelligence

* Scene classification (e.g. indoor, landscape, people)
* Similarity search ("find photos like this")
* Duplicate detection

### Phase 4 – Advanced (Optional)

* Face clustering and labeling (opt-in)
* Event grouping
* Video frame sampling and tagging

---

## Why This Exists

Most people have thousands of photos and videos scattered across folders, devices, and naming conventions. Existing tools are often cloud-based, opaque, or limited.

This project is:

* **Local-first** (your data stays on your machine)
* **Transparent** (you control the pipeline)
* **Extensible** (add your own analysis later)

---

## Requirements

* Python 3.11+
* ExifTool available on `PATH`

Install ExifTool:

* macOS: `brew install exiftool`
* Ubuntu/Debian: `sudo apt-get install libimage-exiftool-perl`
* Windows: install ExifTool and ensure `exiftool` is available in terminal

---

## Installation

```bash
pip install -e ".[dev]"
```

---

## Run

```bash
python -m photo_archive.cli scan /path/to/root \
  --db-path data/db/photo_archive.duckdb \
  --export-path data/exports/photo_metadata.parquet
```

By default, scans are **incremental**: only `new` and `changed` files are sent to ExifTool. Unchanged files reuse previously stored metadata.

The CLI now prints structured progress lines with timestamp, topic, purpose, expectation, and stage duration.
Final output also includes a `Comparison line` with mode, duration, extraction-attempt ratio, and `new+changed` count to compare incremental vs full-rescan runs.

### Force full re-extraction

```bash
python -m photo_archive.cli scan /path/to/root \
  --db-path data/db/photo_archive.duckdb \
  --full-rescan
```

To suppress progress lines:

```bash
python -m photo_archive.cli scan /path/to/root --quiet-progress
```

### Dry run (scan only)

```bash
python -m photo_archive.cli scan /path/to/root --dry-run
```

### Report from latest scan

```bash
python -m photo_archive.cli report --db-path data/db/photo_archive.duckdb
```

The report command prints:

- latest scan summary
- changed/new/missing counts
- failed files list
- non-null coverage by column

Optional flags:

```bash
python -m photo_archive.cli report \
  --db-path data/db/photo_archive.duckdb \
  --scan-id scan_20260330T123717_65c7fea4 \
  --failed-limit 100 \
  --coverage-sort asc
```

### Custom extensions

```bash
python -m photo_archive.cli scan /path/to/root \
  --extension .jpg --extension .jpeg --extension .png
```

## Tiny Streamlit Explorer

Install UI extras:

```bash
pip install -e ".[ui]"
```

Run:

```bash
streamlit run src/photo_archive/streamlit_explorer.py
```

What it shows:

- available fields in your selected table
- non-null count and non-null percentage per field
- least-populated fields
- sample rows for quick inspection

## Incremental Indexing Notes

- `file_metadata` keeps the latest state per file (`new`, `changed`, `unchanged`, `missing`)
- `scans` records one row per completed scan run with delta counts

---

## Output Schema (Phase 1)

A single wide table (`file_metadata`) is used in Phase 1. It includes:

* file identity and filesystem fields
* extracted and normalized metadata fields
* raw ExifTool JSON payload
* extraction status and error fields
* filename parsing fields

Raw metadata is preserved as JSON to allow future enrichment without reprocessing files.

---

## Tech Stack (Phase 1)

* Python
* DuckDB (local analytical database)
* ExifTool (metadata extraction)
* ffprobe (video metadata, future phase)
* Pandas / Polars (data processing)
* OpenCV (thumbnails, future phase)

---

## Project Structure (Planned)

```
media-metadata-explorer/
├── data/                # database + artifacts
├── src/
│   ├── scanner/        # folder scanning
│   ├── extractors/     # metadata extraction
│   ├── parsers/        # filename parsing
│   ├── db/             # database logic
│   ├── thumbnails/     # thumbnail generation
│   └── utils/
├── scripts/
│   └── run_indexing.py
├── notebooks/          # experiments
└── README.md
```

---

## Roadmap

* [x] Phase 1: metadata ingestion pipeline
* [ ] Robust metadata extraction across formats
* [ ] Incremental re-indexing
* [ ] UI (Streamlit)
* [ ] Map + timeline sync
* [ ] Image embeddings
* [ ] Face clustering (opt-in)

---

## Design Principles

* **Metadata-first**: structure before AI
* **Local-first**: no cloud dependency
* **Modular**: each step is replaceable
* **Progressive complexity**: simple → powerful

---

## Contributing

This is currently a personal project, but ideas and suggestions are welcome.

---

## License

TBD
