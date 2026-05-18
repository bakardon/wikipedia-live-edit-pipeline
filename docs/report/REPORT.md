# Wikipedia Edit Pipeline — Final Report

**Course:** Data Engineering
**Project:** Hybrid Data Engineering Pipeline (Batch + Stream)
**Title:** *Wikipedia Live Edit Stream — Trending Topics + Vandalism Detection*

**Group members:**
- Abubakr Bin Mazhar — 2022039
- Aqeeb Warraich — 2022349
- Nabeel Oraya — 2022475

**Submission artefacts:**
- Code: `P_2022039_2022349_2022475.zip`
- Report: `P_2022039_2022349_2022475.pdf`

---

## 1. Dataset Description

### Streaming source — Wikimedia EventStreams (`recentchange`)

- **Endpoint:** `https://stream.wikimedia.org/v2/stream/recentchange`
- **Protocol:** Server-Sent Events (SSE) — long-lived HTTP `text/event-stream`
- **Volume:** ~30–100 events/second, ~2.5–8 million events/day across all Wikimedia projects
- **Coverage:** every edit on every Wikipedia, Wiktionary, Wikidata, Wikibooks, Commons, etc.
- **Schema (per event, JSON):**

  | Field | Type | Description |
  |---|---|---|
  | `id` | int | Internal event id |
  | `type` | string | `edit` / `new` / `log` / `categorize` |
  | `timestamp` | int (epoch s) | When the edit happened |
  | `wiki` | string | `enwiki`, `dewiki`, `commonswiki`, … |
  | `title` | string | Page title |
  | `namespace` | int | 0 = Article, 1 = Talk, 2 = User, … |
  | `user` | string | Username or IP (for anons) |
  | `bot` | bool | Bot flag |
  | `minor` | bool | Minor edit flag |
  | `comment` | string | Edit summary |
  | `revision` | `{old, new}` | Revision IDs |
  | `length` | `{old, new}` | Byte length before/after |

- **No API key, no rate limit, fully public.** Wikimedia requires only a descriptive `User-Agent` header — supplied via `wiki-edit-pipeline/0.1 (university DE final project; …)`.

### Batch source — Wikipedia Clickstream

- **Source:** `https://dumps.wikimedia.org/other/clickstream/`
- **File used:** `clickstream-enwiki-2026-04.tsv.gz` (English Wikipedia, April 2026)
- **Compressed size:** ~478 MB · **Uncompressed:** ~2 GB · **Rows:** ~30–35 million
- **Far above the assignment's 10 k batch-records minimum** (≈3,000× the threshold).
- **Structure (TSV):** `referer ⇥ target ⇥ nav_type ⇥ nav_count`

  | Column | Description |
  |---|---|
  | `referer` | Source page or one of `other-search` / `other-external` / `other-empty` / `other-internal` / `other-other` |
  | `target` | Destination page |
  | `nav_type` | `link` (internal hyperlink), `external` (from outside Wikipedia), `other` |
  | `nav_count` | Number of navigations (≥10 due to upstream privacy filtering) |

- **Why this batch source matches the project narrative:** it gives a historical pageview-popularity baseline. Comparing it to live edit volume produces an anomaly score — pages getting edited disproportionately to their normal traffic are likely breaking-news topics. This is the same signal newsrooms (Reuters, AP) use Wikipedia edit spikes for.

---

## 2. System Architecture

```
                Wikimedia EventStreams (SSE, 30–100 evt/s)
                                │
                                ▼
                ┌─────────────────────────────────┐
                │  ingestion/sse_to_kafka.py      │  Python; User-Agent + Pydantic validation
                │  • reconnect w/ exp. backoff    │
                │  • drop non-edit events         │
                └─────────────────────────────────┘
                                │
                                ▼
                        Kafka topic  wiki.edits
                        (3 partitions · KRaft mode · LZ4)
                                │
                                ▼
                ┌─────────────────────────────────────────────┐
                │  streaming/stream_job.py                     │  Spark Structured Streaming
                │  • read Kafka · parse Wikimedia JSON          │  --master local[2]
                │  • derive editor_type, ip_class               │
                │  • foreachBatch → PostgreSQL JDBC sink        │
                │  • 10-second micro-batch trigger              │
                └─────────────────────────────────────────────┘
                                │
                                ▼
                        PostgreSQL 16
                        ────────────────
                        raw.stg_edit         (append-only landing)
                        raw.stg_clickstream  (append-only landing)
                        core.dim_time        (DDL-seeded, 525 k minute buckets for 2026)
                                │
                                │   dbt run (incremental + table materializations)
                                ▼
                        core.dim_wiki, core.dim_page, core.dim_editor
                        core.fact_edit  (incremental, unique_key = edit_id)
                        core.fact_clickstream
                                │
                                │   dbt run (window-function marts)
                                ▼
                        marts.trending_per_minute   ──┐
                        marts.edit_velocity            │── window functions
                        marts.vandalism_candidates  ──┘
                                │
                                ▼
                        Streamlit dashboard (5 tabs, autorefresh 5 s)


        Batch path (independent, runs on demand):

                Wikipedia Clickstream TSV.gz (478 MB)
                                │
                                ▼
                ┌─────────────────────────────────┐
                │  batch/clickstream_load.py      │  Spark batch — JDBC append
                └─────────────────────────────────┘
                                │
                                ▼
                        raw.stg_clickstream → dbt → core.fact_clickstream
```

**Five layers, mapped to deliverables:**

| Layer | Artefact |
|---|---|
| Ingestion | `ingestion/sse_to_kafka.py` + Kafka |
| Processing | `streaming/stream_job.py` (Spark Structured Streaming) + `batch/clickstream_load.py` (Spark batch) + dbt SQL transforms |
| Modelling | Kimball star: 4 dimensions + 2 facts. Logical model: `warehouse/er_diagram.mmd` (Mermaid). Physical model: `warehouse/ddl/*.sql` + dbt-generated schemas |
| Storage | PostgreSQL 16 (Docker volume `postgres-data`); BRIN time indexes + B-tree partitioning-column indexes |
| Serving | Streamlit dashboard (`dashboard/app.py`) with live + batch tabs |

---

## 3. Implementation Details

### 3.1 Tools

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11 | All application code |
| Apache Kafka | 3.7.0 (KRaft, single broker) | Stream buffer between ingestion and processing |
| Apache Spark | 3.5.3 (`--master local[2]`) | Both streaming AND batch processing (single engine for both) |
| Spark Structured Streaming | 3.5 | Real-time micro-batch consumer of Kafka |
| PostgreSQL | 16 | Storage layer (raw + Kimball star + marts) |
| dbt-postgres | 1.9 | Transformations, tests (data contracts), and DataOps |
| Streamlit | 1.36 | Serving / dashboard |
| Plotly | 5.22 | Charts (dark theme, geographic scatter, donut, stacked area) |
| Docker Compose | v2.40 | Container orchestration |
| OrbStack | 2.1 (macOS) | Local Docker engine |
| GitHub Actions | n/a | CI: `dbt run` + `dbt test` + `pytest` on every push |

### 3.2 Key code modules

- **`ingestion/sse_to_kafka.py`** — connects to the Wikimedia SSE feed with a custom `User-Agent`, validates each event with the Pydantic model in `ingestion/schema.py` (drops non-edit types like `log`, `categorize`), and publishes the *original* Wikimedia JSON to Kafka (preserves replay value). Exponential backoff on disconnect. Configurable cap (`MAX_EVENTS_PUBLISHED`) for demo control without altering pipeline logic.

- **`streaming/stream_job.py`** — Spark Structured Streaming job. Reads the Kafka topic, parses the Wikimedia event with an explicit `StructType` schema, derives `is_anon` (via regex IP check), `is_bot` (event flag), `editor_type`, computes `bytes_changed = length.new − length.old`, and writes to `raw.stg_edit` via `foreachBatch` JDBC append. `?stringtype=unspecified` on the JDBC URL lets Postgres infer the `JSONB` column type for `raw_payload`. Micro-batch trigger = 10 s.

- **`batch/clickstream_load.py`** — Spark batch reader for the gzipped TSV, with an explicit schema (avoids inference cost), filters `nav_count > 0`, tags rows with `wiki_db` + `dump_month`, and JDBC-appends to `raw.stg_clickstream`.

- **`warehouse/ddl/*.sql`** — physical model: schemas (`raw`, `core`, `marts`), raw landing tables, the seeded `core.dim_time` (~525 k minute rows for 2026), and BRIN/B-tree indexes on raw tables.

- **`dbt/`** — dbt project with three model groups:
  - `staging/` (views over raw, light enrichment: `editor_type`, `ip_class_v4`, `minute_bucket`, `is_revert` heuristic)
  - `core/` (Kimball: `dim_wiki`, `dim_page`, `dim_editor`, `fact_edit`, `fact_clickstream`)
  - `marts/` (window functions — see §4.4)

  Dimension surrogate keys are **deterministic md5-based 64-bit hashes** of the natural key — stable across full rebuilds, so `fact_edit` (incremental) stays referentially valid when dimensions are dropped and recreated.

- **`dashboard/app.py`** — Streamlit with five tabs:
  1. ⚡ **Live Pulse** — 8 KPI scorecards (with deltas vs previous window), live edit timeline (users / anon / bots stacked area), world map (Plotly scatter_geo at language centroids), top wikis bar, namespace donut, editor mix donut, recent-edits ticker
  2. 📈 **Trending & Velocity** — outputs of the RANK and LAG window-function marts
  3. 🛡️ **Vandalism** — the 10-minute sliding-window heuristic with severity badges
  4. 🔀 **Stream vs Batch** — live-vs-historical anomaly score (the assignment's "comparison" deliverable)
  5. 🩺 **Pipeline Health** — row counts, freshness, architecture ASCII

  Sidebar: time window, wiki filter, namespace filter, editor-type filter, auto-refresh slider.

### 3.3 DataOps

- **`dbt test`** runs 61 data tests on every push:
  - 10 `unique` (PKs)
  - 25 `not_null`
  - 18 `relationships` (FK integrity between fact and dim tables)
  - 8 `accepted_values` (e.g., `editor_type IN ('user','anon','bot')`)
  These act as **data contracts** between the streaming/batch jobs and downstream marts.
- **`.github/workflows/ci.yml`** spins up a Postgres service, applies DDL, runs `dbt parse`, `dbt run`, `dbt test`, and `pytest` — failing fast on schema or test regressions.
- **`pytest`** covers the producer's `EditEvent.from_event` validation logic.
- **`ruff`** lints all Python.

---

## 4. Processing Logic

### 4.1 Streaming logic

The Spark Structured Streaming job consumes one Kafka micro-batch every 10 seconds. For each batch:

1. **Parse JSON** with the Wikimedia `recentchange` schema (`from_json` with a `StructType`).
2. **Filter** to `type ∈ {edit, new}` and require non-null `title` + `namespace`.
3. **Derive**:
   - `is_anon` ← user matches IPv4/IPv6 regex
   - `bytes_changed` ← `length.new − length.old` (0-default if absent)
   - `ingested_at` ← `current_timestamp()` (for watermarking and incremental dbt)
4. **Sink** via `foreachBatch` → JDBC append to `raw.stg_edit`, batch size 1000.

`startingOffsets=latest`, `failOnDataLoss=false`, `maxOffsetsPerTrigger=5000` — bounded micro-batches keep memory steady on a 16 GB laptop.

### 4.2 Batch logic

Spark reads the gzipped TSV with `compression=gzip` and an explicit schema, applies a `nav_count > 0` filter (Wikipedia's upstream already filters `<10`), and writes JDBC-append in batches of 10 000 rows. The job is **idempotent on `(referer, target, nav_type, dump_month)`** at the dbt layer via a unique index.

### 4.3 Data modelling

Two artefacts before any data was stored:

- **Logical model** (`warehouse/er_diagram.mmd`, Mermaid ER): four dimensions, two facts, 8 relationships.
- **Physical model** (`warehouse/ddl/*.sql`): schemas, raw landing tables with `JSONB` payload columns, the pre-seeded `dim_time`, BRIN indexes on time, B-tree on `(partition_col, ts)` tuples for the window-function partitions.

The remaining `core.*` and `marts.*` tables are dbt-built; their DDL lives in the `.sql` model files. **Surrogate keys are deterministic md5 hashes**, not `SERIAL` — this lets `fact_edit` (incremental, preserves rows across runs) stay referentially valid when dimensions are dropped and rebuilt.

### 4.4 Window functions (used non-trivially)

Three different window functions are used, each in its own dbt model:

1. **`RANK()` — Top trending pages per minute** (`marts.trending_per_minute.sql`)
   ```sql
   RANK() OVER (PARTITION BY minute_bucket ORDER BY edit_count DESC) AS rank_in_minute
   ```
   For every 1-minute bucket, every (wiki, page) gets a global rank by edit count. Filters: namespace = 0 (articles), not bots. Output: rolling top-10 per minute across all wikis.

2. **`LAG()` — Edit-velocity acceleration** (`marts.edit_velocity.sql`)
   ```sql
   LAG(edit_count, 60) OVER (PARTITION BY page_id ORDER BY minute_bucket) AS edits_lag_60
   ```
   Compares the current minute's edit count for a page to the same page **60 minutes earlier**. The difference (`accel = edit_count − edits_lag_60`) is a breaking-news signal: a small page that suddenly gains edit traffic has high acceleration even at low absolute volume.

3. **Sliding window — Vandalism heuristic** (`marts.vandalism_candidates.sql`)
   Two overlapping 10-minute windows per 5-minute boundary, grouped by `(page, ip_class_v4)`. Surfaces `(page, window)` pairs with ≥3 anonymous edits OR ≥30% revert share. Wikipedia patrollers use the same signature.

   The window construction uses `date_trunc('minute', ts) - (EXTRACT(MINUTE FROM ts)::int % 5) * interval '1 minute'` to bucket each edit into its 5-minute "anchor" and then aggregates over `[anchor, anchor + 10 min)`.

---

## 5. Results & Insights

### 5.1 Patterns discovered (run of ~30 minutes)

- **Bots are most edits.** With the firehose unfiltered, bot edits typically outnumber human edits by ~2:1 (mostly Wikidata bots editing Q-items, archive bots, link bots).
- **Wikidata dominates by edit volume** but enwiki dominates by anonymous edits (more open to drive-by IP editing).
- **Anonymous share** sits around 8–12 % of edits — the rest are users + bots.
- **Top-namespace mix:** ~70 % articles, ~10 % user talk, ~7 % file (mostly Commons), the rest smaller.

### 5.2 Real-time behaviour

The pipeline keeps up easily with the firehose:

- Producer: ~22 edits/sec published after filtering (~57 raw events/sec received).
- Kafka: 3 partitions, single broker, sub-millisecond latency.
- Spark Structured Streaming: 10 s micro-batch → rows landing in Postgres within 10–12 s of the original Wikipedia edit.
- Dashboard: 5-second autorefresh; the **Live Pulse** tab's edit-rate line chart visibly ticks up between refreshes.
- 61 dbt tests run in 0.92 s; the full warehouse + marts rebuild in <1 s for hundreds of thousands of rows.

### 5.3 Comparison of batch vs stream outputs

The **🔀 Stream vs Batch** tab computes an anomaly score per page:
```
anomaly = live_edits (last 1h) ÷ historical_navigations (Clickstream)
```

Pages with **high anomaly** are being edited disproportionately to their normal pageview traffic — strong candidates for breaking-news events. This is the operational utility newsrooms see in Wikipedia edit spikes.

Some structural differences between the two outputs:

| Aspect | Streaming output | Batch output |
|---|---|---|
| Latency | seconds | monthly |
| Granularity | individual edits | aggregated nav counts (≥10) |
| Coverage | all wikis (multilingual) | one wiki (enwiki) at a time |
| Volume (one demo run) | ~10–100 k edits | tens of millions of nav rows |
| Use case | trending / breaking-news / vandalism | popularity baseline |

Both are necessary: streaming alone has no concept of "is this page usually busy?"; batch alone has no concept of "is something happening *now*?". The dashboard's anomaly score is their join.

---

## Appendix A — Repo layout

```
.
├── docker-compose.yml          # 5 services + 1 on-demand (batch)
├── Makefile                    # make up / make stream / make batch / make dbt
├── .env / .env.example
├── ingestion/                  # SSE → Kafka producer
├── streaming/                  # Spark Structured Streaming job
├── batch/                      # Spark batch job + download script
├── warehouse/                  # DDL + Mermaid ER
├── dbt/                        # dbt project + macros + 10 models + 61 tests
├── dashboard/                  # Streamlit app (5 tabs)
├── .github/workflows/ci.yml    # CI pipeline
├── tests/                      # pytest unit tests
└── docs/report/REPORT.md       # this file
```

## Appendix B — How to run

```bash
cp .env.example .env
make up                  # kafka + postgres + producer + streaming + dashboard
make download-clickstream
make batch
make dbt && make dbt-test
open http://localhost:8501
```
