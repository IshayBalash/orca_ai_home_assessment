# Ship Positions Pipeline — Solution

## How to Run

### Prerequisites

```bash
pip install -r requirements.txt
```

### Run the pipeline

From the project root (`orca_ai_home_assessment/`):

```bash
python src/run_me.py
```

This will process all records in `raw.ship_positions` day by day from `2026-01-28` to `2026-02-22` and write enriched records into `silver.ship_positions`.

### Incremental run (after appending new data)

After merging `ship_positions_incremental.db` data into the source, re-run the same command. The pipeline will automatically process only new records that were not already written to the target table.

---

## Architecture Overview

```
run_me.py
  └── pipe.py (orchestrator)
        ├── Delta     → extract raw batch from source (SQL)
        ├── Dqa       → data quality checks and deduplication
        └── Publish   → enrich with derived fields and write to silver
```

The pipeline runs in **daily chunks** (one call to `run_pipe` per day). Each call extracts a 1-day window from the source, cleans it, enriches it, and appends only new records to the target.

### Configuration
DB paths are driven from `src/config/config.yaml`. Schema and table names are hardcoded in the SQL files (`sql/`) — this should be improved; requires research into how DuckDB handles dynamic identifiers.
---

## Stage Breakdown

### 1. Extract (`Delta`)

Extracts a time-windowed batch from `raw.ship_positions` using named SQL parameters (`$start_ts`, `$end_ts`).
The raw `time` column is a Unix epoch float and is converted to a proper timestamp in the SQL layer via `to_timestamp(time)`.

Also fetches the **last timestamp per ship from the silver table** (`MAX(time_ts)` per ship). This watermark is used in the next stage to filter out records that were already processed or are out of order.

### 2. Data Quality (`Dqa`)

All checks are isolated static methods. The main `dqa()` method runs them in sequence:

| Check | Decision |
|---|---|
| NULL in `lat`, `lon`, `sog`, `cog` | Drop — cannot enrich or use a record with missing position or motion data |
| `lat` outside −90/90 or `lon` outside −180/180 | Drop — physically impossible coordinates |
| `sog < 0` | Drop — negative speed is sensor noise |
| `cog` outside 0–360° | Drop — invalid heading |
| `time <= max(time_ts)` in silver (per ship) | Drop — already processed; ensures idempotent incremental loads |

The `time <= max(time_ts)` filter is applied per ship: a ship that is new to the target is fully kept. Only records with a timestamp already present in silver are filtered out.

### 3. Enrich & Publish (`Publish`)

#### Seed row pattern

To correctly compute lag-based fields (ROT, acceleration, distance) for the **first record of each ship in a batch**, we fetch the last known row per ship from silver (`sql/get_last_row_per_ship.sql`) and prepend it to the batch before computing shifts. These seed rows are dropped before writing.

This ensures that even the first record of a new batch gets correct enrichment values, not NaN, as long as a prior record exists in silver. New ships with no history still get NaN on their first record, which is correct.

#### Write

`elt_published_at` (UTC timestamp) is added at write time. Records are inserted into the target table using DuckDB's ability to reference a pandas DataFrame directly:

```python
conn.execute("INSERT INTO silver.ship_positions SELECT * FROM df")
```
---

## What's Next

- **Batch size is 1 day** — This is an arbitrary choice. In production, the batch size should be driven by analytics needs and data volume, and `run_pipe` should be triggered by a scheduler (e.g. Airflow, cron).

- **SQL files have hardcoded table names** — All database names, schema names, and table names should be driven from the config file. This requires research into how DuckDB handles dynamic identifiers, since named parameters only work for values, not table or schema names.

- **Enrichment logic uses Python instead of SQL** — The post-processing step uses pandas, which is not necessarily the best approach. In a more advanced SQL environment, much of this could be done in SQL. Additionally, operations that need data from both the source and target databases (e.g. the seed row pattern) were done in Python because working across two separate DuckDB files in a single query is not straightforward. A shared SQL environment would simplify this significantly.

- **Algoritem validations** - ensure that both the input data validations and the calculation logic used to produce the derived field are correct. 


