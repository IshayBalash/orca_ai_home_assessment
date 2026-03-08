# Data Engineer Assignment: Ship Positions Pipeline

## Overview

In this assignment, you will design and implement a **production-grade data pipeline** that processes ship sensor data and builds an analytical table.

You are provided with a **pipeline skeleton** (folders, class signatures, and method stubs). It illustrates the intended architecture, but you are free to modify or extend it as needed.

Approach this as if you were building a real production pipeline:

- What makes it robust?
- How do you prevent data corruption?
- How do you guarantee safe incremental processing?
- How do you make it configurable and testable?

The pipeline need to support **incremental loads** 

**Time Estimation:** ~2–3 hours (starting after our pre-implementation discussion)
---

# Working with AI Tools

Using AI effectively is part of the evaluation.

You are expected to use an AI coding assistant (ChatGPT, Claude Code, etc.) throughout the task. We evaluate *how* you work with AI — not just the final code.

Treat the AI as a **thinking partner**, not a code generator.

## Expectations

### 1. Plan Before Prompting
- Collaborate with the AI to design your architecture before writing code.
- Discuss trade-offs and responsibilities of each pipeline stage.
- The planning conversation must be included in your submission.

### 2. Think Critically
- Do not blindly accept AI outputs.
- Review, question, and validate everything before applying it.

### 3. Work Iteratively
- Prompt → evaluate → refine → improve.
- Show evidence of reasoning.

### 4. Own Your Solution
You must be able to explain and justify every decision during review.  
If you cannot explain it, do not submit it.

> You must submit the full AI transcript — including the planning phase, not just implementation prompts.

---

# What You Receive

You receive a pre-built skeleton with:

- `data/raw/ship_positions.db` — The source table
- `data/raw/ship_positions_incremental.db` — an increnemtal table used to test incremental load. It contains a continuation of the data in `data/raw/ship_positions.db` and should be used to verify that your pipeline correctly processes only new records when run against an updated source.
- Folder structure
- Class definitions
- Method stubs

The structure is a recommendation — you may extend or refactor it if needed.

---

# Project Structure

## `elt_utils/` — Reusable ELT Framework

This layer should be generic and reusable across pipeline jobs.

You must implement the logic in each module:

- `elt_utils/db/duckdb.py` — DuckDB wrapper to work with the DB
- `elt_utils/transform/config.py` — Configuration loader
- `elt_utils/transform/delta.py` — Incremental extraction logic
- `elt_utils/transform/dqa.py` — To handle Data quality issues
- `elt_utils/transform/publish.py` — Target table publishing logic

---

## `src/` — Ship Positions Pipeline Job

This is the job-specific implementation built on your framework.

- `src/run_me.py` — Entry point
- `src/pipeline/pipe.py` — Orchestrates all pipeline stages
- `src/bl/bl.py` — Business logic and derived features

# Source Dataset

## Table: `raw.ship_positions`

The `raw.ship_positions` table contains voyage tracking data for **two vessels**.
Each ship records its geographic position and navigation metrics throughout its journey.

Each row represents a single **sensor snapshot**, including:

- The ship name (vessel identifier)
- The record timestamp
- The ship’s geographic location (latitude/longitude)
- Its speed over ground (SOG)
- Its course over ground (COG)

Over a full voyage, this results in **thousands of records per ship**, allowing you to reconstruct:

- The vessel’s route
- Its speed patterns
- Direction changes
- Behavioral patterns over time

This dataset represents the **raw sensor feed** from the ships.

In a real-world scenario, this table would be **continuously updated** as new sensor readings are transmitted from the vessels. Your pipeline should therefore assume that new records are constantly appended and must be processed incrementally.

## Schema

| Column | Type | Notes |
|---|---|---|
| `ship_name` | VARCHAR | Vessel identifier |
| `time` | DOUBLE | Epoch timestamp in seconds (float) |
| `lat` | FLOAT | Latitude in decimal degrees, −90 to 90 |
| `lon` | FLOAT | Longitude in decimal degrees, −180 to 180 |
| `sog` | FLOAT | Speed over ground in knots |
| `cog` | FLOAT | Course over ground, 0–360° |

> **The data is not clean.** Part of your job is to identify what is wrong with it and decide how each issue should be handled.

---

# Target Table

## `silver.ship_positions`

The final table should include the following schema:

| Column | Description                                                                                   | Notes |
|--------|-----------------------------------------------------------------------------------------------|-------|
| `ship_name` | Vessel identifier                                                                             | |
| `time_ts` | recorded time (in timestamp format)                                                           | |
| `lat` | Cleaned latitude in decimal degrees                                                           | |
| `lon` | Cleaned longitude in decimal degrees                                                          | |
| `sog` | Speed over ground in knots (cleaned)                                                          | |
| `cog` | Course over ground in degrees (cleaned)                                                       | |
| `rot` | Rate of Turn — change in COG between consecutive records for the same vessel                  | Calculated field |
| `acceleration` | Change in SOG between consecutive records for the same vessel                                 | Calculated field |
| `distance_traveled` | Distance from the previous position for the same vessel in nautical miles (Haversine formula) | Calculated field |
| `elt_published_at` | Timestamp set at publish time                                                                 | Operational Field |

### Stretch Enrichments

| Field               | Meaning                | Formula                                              |
|---------------------|------------------------|------------------------------------------------------|
| `rot`               | Rate of Turn (degrees) | `COG_current − COG_previous` with 360° wrap-around   |
| `acceleration`      | Acceleration (knots)   | `SOG_current − SOG_previous`                         |
| `distance_traveled` | Distance traveled (nm) | Haversine formula, Earth radius = 3440.065 nm        |
# What You Need to Deliver

## 1. Working Code

Implement the pipeline so that running it end-to-end:

- Reads from `raw.ship_positions` incrementally (processing only new records on each run)
- Handles all data quality issues in the source data
- Applies all business logic and enrichments
- Publishes clean, validated records into `silver.SHIP_POSITIONS`

## 2. Target Table Output

Submit the populated `silver.ship_positions` table as a DuckDB file at:

```
data/silver/ship_positions.db
```

This file must reflect the **consolidated result** of running the pipeline twice:
1. First against `ship_positions.db` (full load)
2. Adding into `ship_positions.db` the new data from `ship_positions_incremental.db` and run the pipeline again to process the new records
  

## 3. AI Planning Artefacts

- **Your initial plan** — the structured plan you created with the AI before writing any code (architecture, stage responsibilities, design decisions)
- **Full AI conversation transcript** — the complete chat history including the planning phase, implementation prompts, and any refinements. Do not trim it.

## 4. Pipeline Documentation

A `README.md` in the project root explaining:

- How to run the pipeline
- What each stage does and why
- How data quality issues are handled and what decisions were made
- Any assumptions or trade-offs

## 5. Live Presentation

Be prepared to walk through your code and answer questions on:
