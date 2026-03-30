# AGENTS.md

## Project
Local-first photo and media archive indexer for large personal collections.

The tool scans a chosen root folder and all subfolders, extracts metadata from image files (and later video files), stores the results in a queryable local database/table, and supports filtering, search, gallery viewing, and time/place exploration.

This project should be built incrementally. The first milestone is **Phase 1: reliable ingestion and metadata extraction**.

---

## Product vision
The goal is to turn a messy photo collection into a searchable, inspectable archive.

A user should be able to:
- scan a folder recursively
- collect metadata from each media file
- inspect one row per file in a table-like structure
- filter/search by date, place, device, folder, file type, and other attributes
- open a gallery of matching photos
- later explore media on a timeline and map
- later enrich the archive with image/video analysis, clustering, and similarity search

This is a **local-first** tool. The default assumption is that the user's media stays on their own machine.

---

## Core principles

### 1. Metadata first
Before adding AI features, get the metadata layer right.

### 2. Local first
Prefer local files, local database, and local processing. Avoid cloud dependencies unless explicitly needed.

### 3. Modular pipeline
Separate:
- file discovery
- metadata extraction
- normalization
- persistence
- querying
- preview generation
- later analysis

### 4. Preserve raw data
Do not only store normalized columns. Also keep the raw metadata payload per file so new fields can be derived later without rescanning if possible.

### 5. Incremental development
Build a useful Phase 1 before attempting dashboards, facial recognition, semantic search, or video understanding.

### 6. Robust over elegant
Metadata across media formats is inconsistent. Prefer traceability, logging, and explicit fallbacks over brittle assumptions.

---

## Target scope

### Phase 1: ingestion + metadata extraction
Build a script or small local app that:
- scans a folder recursively
- identifies supported image files
- extracts filesystem metadata
- extracts embedded metadata using mature external tools/libraries
- optionally parses filename patterns
- stores one row per file in a local database/table
- supports export to CSV or Parquet for inspection

This phase should not try to solve everything. It should establish a dependable indexing foundation.

### Later phases
- thumbnail generation
- basic UI for filtering/search
- gallery view
- timeline + map dashboard
- video metadata extraction
- video frame sampling
- image classification/tagging
- duplicate detection
- similarity search
- face clustering and optional person labeling

---

## Recommended technology choices

### Primary language
**Python**

Reason:
- excellent filesystem and data tooling
- strong ecosystem for metadata, media processing, and local apps
- easy to prototype and extend
- suitable for scripts, notebooks, and local apps

### Metadata extraction
Use **ExifTool** as the primary extractor for image metadata.

Reason:
- broad format support
- handles EXIF, IPTC, XMP, maker notes, GPS, and many vendor-specific fields
- more reliable than trying to hand-roll extraction across formats

Use **ffprobe** later for video metadata.

### Database
Use **DuckDB** first.

Reason:
- local single-file database
- SQL-friendly
- great fit for analytical queries and export workflows
- easy to use with Pandas or Polars

SQLite is acceptable, but DuckDB is preferred for analytics-heavy workflows.

### Data manipulation
Use **Pandas** or **Polars**.

### UI later
For the first UI, use **Streamlit**.

---

## Proposed architecture

### A. Scanner
Responsibility:
- walk the directory tree
- identify supported file types
- collect filesystem facts
- detect added/changed/missing files

Inputs:
- root folder path
- supported extensions

Outputs:
- list of discovered files

### B. Extractor
Responsibility:
- invoke ExifTool on batches of files
- parse returned metadata
- extract normalized fields
- store raw JSON payload

Outputs:
- normalized metadata rows
- raw metadata payloads
- extraction warnings/errors

### C. Normalizer
Responsibility:
- reconcile inconsistent field names
- derive best timestamp and best GPS fields
- infer fallback values from filename or filesystem metadata
- standardize units and null handling

### D. Persistence layer
Responsibility:
- write rows into DuckDB
- preserve scan history if desired
- support export for debugging

### E. Validation/reporting
Responsibility:
- summarize extraction completeness
- show which fields are often empty
- report unknown patterns and extraction failures

This layer is important because metadata extraction will indeed be messy.

---

## Data model for Phase 1

Use a pragmatic schema. It does not need to be perfect.

### Table: `files`
One row per file.

Suggested columns:
- `file_id` - stable identifier, e.g. hash of path or content hash later
- `path`
- `parent_folder`
- `filename`
- `extension`
- `media_type`
- `size_bytes`
- `fs_created_at`
- `fs_modified_at`
- `scan_root`
- `scan_time`
- `is_supported`
- `extract_status`
- `extract_error`

### Table: `metadata`
One row per file, normalized fields.

Suggested columns:
- `file_id`
- `captured_at`
- `captured_at_source`
- `gps_lat`
- `gps_lon`
- `gps_alt`
- `camera_make`
- `camera_model`
- `lens_model`
- `software`
- `width`
- `height`
- `orientation`
- `color_space`
- `iso`
- `f_number`
- `exposure_time`
- `focal_length`
- `artist`
- `copyright`
- `keywords`
- `title`
- `description`

### Table: `filename_parse`
Optional but useful.

Suggested columns:
- `file_id`
- `parsed_datetime`
- `parsed_pattern`
- `parsed_event_label`
- `parse_confidence`

### Table: `raw_metadata`
Suggested columns:
- `file_id`
- `tool_name`
- `tool_version`
- `raw_json`

If you want a simpler v1, combine into one wide table plus a raw JSON column.

---

## Normalization strategy
Do not assume one universal metadata key.

For important concepts, define preferred fallbacks.

### Best timestamp priority
Try in this order:
1. EXIF original capture datetime
2. other embedded creation datetime fields
3. parsed filename datetime
4. filesystem created time
5. filesystem modified time

Store both:
- the chosen value
- the source used

### Best GPS priority
Try in this order:
1. embedded EXIF/XMP GPS fields
2. later sidecar files or companion metadata if added
3. leave null

### Device identity
Construct a derived `device_label` later from:
- make
- model
- lens
- software

### Null handling
Empty metadata is expected. Use explicit nulls and log which extractors failed or which fields were unavailable.

---

## Filename parsing
Filename parsing should be additive, not authoritative.

Examples of patterns worth supporting later:
- `IMG_20220304_153012`
- `2021-08-15 17.02.11`
- `PXL_20240201_102030123`
- `WhatsApp Image 2022-03-05 at 19.22.10`
- folder names that imply events or locations

Rules:
- store the parsing result separately
- never overwrite embedded metadata blindly
- record confidence and matched pattern

---

## Supported file types for Phase 1
Start with common image formats only.

Recommended initial support:
- `.jpg`
- `.jpeg`
- `.png`
- `.heic`
- `.tif`
- `.tiff`
- `.webp`

Optional later:
- RAW formats such as `.cr2`, `.nef`, `.arw`, `.dng`
- videos such as `.mp4`, `.mov`, `.avi`, `.mkv`

---

## Risks and known difficulties

### 1. Metadata inconsistency
Different devices and apps write different tags.

### 2. Missing metadata
Some files will have little or no useful embedded metadata.

### 3. Filesystem timestamps are weak proxies
Copying/exporting files can destroy original date meaning.

### 4. GPS is sparse
Many photos will not have location data.

### 5. HEIC and vendor formats can be tricky
Extraction coverage depends on tooling and platform support.

### 6. Videos need a different pipeline
Do not treat them as images too early.

### 7. Duplicate and edited copies complicate interpretation
Same image content may appear in multiple files with different metadata.

---

## Engineering requirements for Phase 1

### Reliability
- never crash the whole scan because of one bad file
- log failures per file
- support reruns

### Observability
Produce reports such as:
- number of files scanned
- number supported
- number extracted successfully
- top file extensions
- percentage of rows with capture date
- percentage with GPS
- percentage with camera model
- top extraction errors

### Reproducibility
- pin dependencies
- document how ExifTool is installed
- keep raw metadata payloads

### Performance
- batch ExifTool calls where practical
- avoid loading full image bytes unless needed
- keep Phase 1 efficient enough for large folders

---

## Suggested repository layout

```text
photo-archive/
  AGENTS.md
  README.md
  pyproject.toml
  .env.example
  data/
    db/
    exports/
    logs/
  src/
    photo_archive/
      __init__.py
      config.py
      scanner.py
      extractors/
        __init__.py
        exiftool_extractor.py
        filename_parser.py
      normalize.py
      database.py
      pipeline.py
      reporting.py
      cli.py
  notebooks/
    metadata_exploration.ipynb
  tests/
    test_filename_parser.py
    test_normalize.py
```

---

## Phase 1 success criteria
Phase 1 is complete when:
- a user can point the tool at a root folder
- the tool recursively scans supported image files
- metadata is extracted without failing the full run
- results are written to DuckDB
- results can be exported to CSV or Parquet
- a summary report shows extraction coverage and failures
- the codebase is ready to extend with thumbnails and UI later

---

## Out of scope for Phase 1
- polished UI
- gallery page
- timeline/map dashboard
- video frame sampling
- semantic image classification
- duplicate detection
- face recognition or identification
- cloud sync
- multi-user workflows

---

## Agent guidance
When working on this repository:
- protect the Phase 1 boundary
- prefer boring, reliable tooling over custom metadata parsing
- preserve raw metadata whenever possible
- make extraction failures visible instead of hiding them
- favor traceable normalization rules
- optimize after correctness and observability

If a design choice conflicts with these principles, choose the option that improves reliability, inspectability, and incremental progress.
