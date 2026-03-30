# Media Metadata Explorer

## Overview

Media Metadata Explorer is a local-first tool designed to bring structure to large, unorganized collections of photos and videos. It scans directories, extracts metadata (EXIF, GPS, device info, timestamps), and stores it in a queryable database.

The goal is simple: make your media collection searchable, explorable, and actually usable.

---

## Features (Planned)

### Phase 1 – Metadata Indexing

* Recursive folder scanning
* Image and video detection
* Metadata extraction (EXIF, filesystem, video)
* Filename parsing for additional context
* Storage in a local database (DuckDB)
* Thumbnail generation

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

* Face clustering and labeling
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

## Tech Stack (Phase 1)

* Python
* DuckDB (local analytical database)
* ExifTool (metadata extraction)
* ffprobe (video metadata)
* OpenCV (thumbnails)
* Pandas / Polars (data processing)

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

## Getting Started (Phase 1)

### 1. Install dependencies

```bash
pip install duckdb pandas opencv-python
```

Install external tools:

* ExifTool: [https://exiftool.org/](https://exiftool.org/)
* ffmpeg (for ffprobe): [https://ffmpeg.org/](https://ffmpeg.org/)

### 2. Run indexing

```bash
python scripts/run_indexing.py --input /path/to/media
```

This will:

* scan all files
* extract metadata
* populate the database
* generate thumbnails

---

## Data Model (Simplified)

### files

* path
* filename
* extension
* size
* created / modified timestamps

### media_metadata

* captured_at
* width / height
* duration (video)
* camera make / model
* GPS coordinates

### thumbnails

* file reference
* thumbnail path

Raw metadata is also stored as JSON for flexibility.

---

## Roadmap

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
