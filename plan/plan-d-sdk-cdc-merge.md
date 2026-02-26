# Plan D: PB SDK — Native Record-Set Input Support

## Context

Plans A-v2 (quickwin + fewdays) incrementalize cs-profiles using existing PB patterns: `is_append_only`, `merge:`, CDC entity types, and rollup models. This works but requires significant project-level boilerplate — new entity types, id stitchers, rollup models, and entity var groups for every CDC table.

Plan D proposes PB SDK features that would make CDC-based incremental computation declarative, eliminating the boilerplate.

## Two Kinds of Inputs

Every input falls into one of two categories:

1. **Event stream**: Each row is an independent event. Rows are never overridden. `count(*)` means "how many events happened." Examples: `app_tracks`, `app_identifies`.

2. **Record set**: Each row represents the current state of a mutable record. Multiple rows exist for the same record (created, updated, deleted), distinguished by a primary key and ordered by a timestamp. `count(*)` means "how many records currently exist." Examples: `app_sources`, `sf_account`, all Stitch-synced tables.

Both are append-only at the **storage** layer (Stitch/Salesforce only INSERT, never UPDATE or DELETE warehouse rows). The difference is in **business semantics**.

PB currently has `is_event_stream` and `is_append_only` but no way to declare record-set semantics.

## The Core Problem

Current PB `merge:` assumes event-stream semantics: each row is independent, so `merge: sum({{rowset.count}})` correctly accumulates. On record-set inputs, this double-counts because the same record appears in multiple batches as it gets updated.

```
-- Event stream merge (correct):
old_count + delta_count = total

-- Record set: same record in both batches
old_count + delta_count = OVERCOUNTED
```

Additionally, records can be soft-deleted (DELETED=TRUE, `_SDC_DELETED_AT`). `merge: sum()` can never decrease a count — a previously-counted record stays counted even after deletion.

---

## Proposed Feature: `override_channel`

### Input Contract

```yaml
# In inputs.yaml — record-set input
- name: app_sources
  contract:
    is_append_only: true
    override_channel: ID           # PK — rows with same ID override each other
    override_ordering: UPDATEDAT   # Latest UPDATEDAT wins
  app_defaults:
    table: RUDDER_TEST_EVENTS.PPW_PUBLIC.SOURCES
    occurred_at_col: UPDATEDAT
    ids:
      - select: WORKSPACEID
        type: workspace_id
        entity: workspace
```

An `override_channel` on an input contract tells PB: "rows in this input override each other — group by this column to deduplicate, keeping the latest by `override_ordering`." Entity vars don't need to change — PB infers the correct behavior from the input they read from.

### DELETED Columns in CDC

Soft deletes are standard in CDC:
- Stitch: `_SDC_DELETED_AT` (system column, set when source record is deleted)
- Source apps: `DELETED` column (business field)
- Salesforce: `IS_DELETED`

All verified in the current dataset — every Stitch table has `_SDC_DELETED_AT`, and SOURCES, DESTINATIONS, DG_SOURCE_TRACKINGPLAN_CONFIGS, WHT_PROJECTS also have a business `DELETED` column.

With `override_channel`, deleted records are handled naturally:
- Record created: row with DELETED=FALSE appended
- Record deleted: row with DELETED=TRUE appended (new `_SDC_BATCHED_AT`)
- Dedup picks the latest row (DELETED=TRUE)
- `WHERE DELETED='FALSE'` correctly excludes it
- Count decreases — correct

### count(DISTINCT) and HyperLogLog

`count(DISTINCT X)` is not composable with `sum()` — the same value in two batches gets counted twice. Two approaches:

1. **With `override_channel`**: Dedup to latest row per record first, then `count(DISTINCT X)` on deduplicated rows. Exact and correct — no HLL needed.

2. **Without `override_channel`** (event streams): Use HyperLogLog for approximate distinct counts across batches. Snowflake supports `HLL_ACCUMULATE` (stores sketch), `HLL_COMBINE` (merges sketches), `HLL_ESTIMATE` (extracts count). This would require PB to support sketch-typed var columns.

Option 1 is strictly better for record-set inputs. Option 2 is needed for event streams with `count(DISTINCT)`.

---

## Unbounded Vars: Invalidate + Recompute

For entity vars WITHOUT time windows (e.g., `count(ID) WHERE DELETED='FALSE'`):

When an input has `override_channel`, PB's incremental strategy for entity vars is **invalidate + recompute for affected entities**:

1. Identify affected entities: any entity with new delta rows
2. For those entities only, re-scan the full input (bounded by entity)
3. Dedup by `override_channel`, keep latest by `override_ordering`
4. Re-evaluate the entity var's `select` + `where` on deduplicated rows
5. **Replace** (not merge) the var value for affected entities

```sql
-- Affected entities: those with new delta rows
affected_entities AS (
    SELECT DISTINCT entity_id FROM deltaInput
),
-- For affected entities: read ALL their records, dedup, re-aggregate
recomputed AS (
    SELECT entity_id, count(ID) as var_value
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY entity_id, {{override_channel}}
            ORDER BY {{override_ordering}} DESC
        ) AS _rn FROM fullInput
        WHERE entity_id IN (SELECT entity_id FROM affected_entities)
    ) WHERE _rn = 1 AND DELETED = 'FALSE'
    GROUP BY entity_id
),
-- Unaffected entities: keep old values
SELECT entity_id, var_value FROM recomputed
UNION ALL
SELECT entity_id, var_value FROM lastEntityVarTable
WHERE entity_id NOT IN (SELECT entity_id FROM affected_entities)
```

This works for **every aggregation type** — no algebraic constraints. The cost is re-reading input rows for affected entities, which is typically a small fraction of the total.

Entity vars don't need `merge:` — PB auto-generates the invalidate+recompute pattern when the input has `override_channel`.

---

## Time-Windowed Vars: Active Section Tables

### Why Algebraic Merge Doesn't Work

Time-windowed vars on record-set inputs face two simultaneous challenges:

1. **Override**: a record's state changes (DELETED, ENABLED, field values) — need to retract old contribution, add new
2. **Window expiry**: records fall out of the window as time passes — need to subtract expired contributions

Algebraic merge (retract + accumulate) works for `sum`/`count` but NOT for `min`/`max`/`count(DISTINCT)`/`avg`. The industry approaches:

| Approach | Works for | Complexity | Used by |
|---|---|---|---|
| Retract + Accumulate | sum, count only | Moderate | Flink, ksqlDB |
| Segment-based windows | sum, count, max, min | High (per-segment storage) | Druid, ClickHouse |
| Invalidate + recompute | Everything | Simple | dbt, most batch systems |
| Per-record changelog | Everything | High (per-record storage) | Materialize |

PB is a batch system. Retraction and segment-based approaches add complexity that doesn't fit the model.

### Solution: Active Section Tables

Instead of trying to algebraically merge time-windowed vars on record-set inputs, **maintain a materialized active section table** — a deduplicated, time-bounded snapshot of the input, refreshed incrementally.

```yaml
# Automatically generated by PB when input has override_channel
# and entity vars use time-windowed WHERE clauses
- name: app_sources_active_90d
  model_type: sql_template
  model_spec:
    materialization:
      output_type: table
    single_sql: |
      -- Union previous active section + delta rows
      -- Dedup by override_channel (keep latest)
      -- Drop records outside 90-day window
      SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY {{override_channel}}
          ORDER BY {{override_ordering}} DESC
        ) AS _rn
        FROM (
          SELECT * FROM {{lastThis}} WHERE {{override_ordering}} >= {{end_time}} - INTERVAL '90 day'
          UNION ALL
          SELECT * FROM {{delta}}
        )
      ) WHERE _rn = 1
        AND {{override_ordering}} >= {{end_time}} - INTERVAL '90 day'
```

Entity vars read from this active section table instead of the raw input:

```yaml
- entity_var:
    name: app_number_sources_created_days_last_90
    select: count(*)
    from: models/app_sources_active_90d   # reads from active section, not raw input
    where: DELETED = 'FALSE' and createdat > {{end_time_sql}} - INTERVAL '90 day'
    # No merge: needed — active section is already deduplicated and bounded
```

### Why This Works

The active section table handles **both** challenges in one place:

1. **Override**: `ROW_NUMBER() PARTITION BY override_channel` deduplicates to latest state per record
2. **Window expiry**: `WHERE ordering >= end_time - 90 day` drops records outside the window
3. **Incremental**: only reads delta + previous active section (not full history)
4. **Any aggregation**: entity vars do a simple aggregate over the table — no merge complexity

### Auto-Generation

PB could auto-generate active section tables when:
- Input has `override_channel`
- Entity vars from that input use time-bounded `where:` clauses
- PB groups vars by (input, max_lookback) to minimize materializations

This is the same concept as Plan C's `lookback` key, but for record-set inputs instead of event streams.

### Comparison with Current Approach

The `recent_app_tracks` and `recent_app_identifies` models are hand-written active section tables for event streams. The `thena_current_state` model is a hand-written active section table for a record set (with dedup). This proposal auto-generates both patterns based on input contract.

---

## Implementation Scope

### Input Contract Changes

```yaml
contract:
  is_append_only: true

  # Event stream (existing):
  is_event_stream: true

  # Record set (new):
  override_channel: <column_name>      # PK of the record
  override_ordering: <timestamp_col>   # Latest wins
```

`is_event_stream` and `override_channel` are mutually exclusive. An input is either an event stream or a record set.

### PB SDK Changes Required

| Area | Change | Effort |
|------|--------|--------|
| Input spec parsing | Parse `override_channel` and `override_ordering` from contract | Small |
| Entity var template | New branch: when input has `override_channel`, generate invalidate+recompute SQL instead of algebraic merge | Moderate (~60-80 lines of template) |
| Active section model | Auto-generate intermediate SQL model for time-windowed vars on record-set inputs | Moderate |
| Validation | If input has `override_channel`, entity vars from it don't need explicit `merge:` | Small |
| Cache invalidation | Include `override_channel` in hash components | Small |

### Template Change Detail

The core change is in `core/entityVarItem/modelCreatorSqlHelper.sql`. When the source input has `override_channel`:

**Unbounded vars** → invalidate + recompute pattern (affected entities only, replace not merge)

**Time-windowed vars** → read from auto-generated active section table (no merge needed)

Both paths avoid the current `rowset` UNION ALL + merge expression pattern entirely.

---

## Interaction with Existing Plans

| Plan | Approach | Status |
|------|----------|--------|
| **A-v2 quickwin** | `is_append_only` + `merge:` on any_value/max only | **Ship now** — correct for composable aggregations |
| **A-v2 fewdays Phase 1** | Hand-written active section tables per CDC input | **Ship now** — same pattern Plan D auto-generates |
| **A-v2 fewdays Phase 2-3** | Decompose views, unify active section pattern | **Still needed** regardless |
| **B (tumbling windows)** | Hand-written intermediate SQL models | **Subsumed** by active section tables |
| **C (lookback key)** | `lookback:` on entity vars, auto-generate time-bounded models | **Complementary** — lookback for event streams, active section for record sets |
| **D (this)** | `override_channel` on inputs, auto invalidate+recompute + active section | **Target state** |

Plan D **auto-generates** what Phase 1 of fewdays hand-writes:
- Active section tables per CDC input (dedup to current state)
- Active section tables with time bounds (for time-windowed vars)

Plan D replaces:
- Hand-written active section SQL models (Phase 1 of fewdays)
- Plan B (hand-written tumbling window models)

Plan D complements:
- Plan C (`lookback` key for event streams)
- Phase 2-3 of fewdays (view decomposition — still needed)

---

## Effort

| Component | Time |
|-----------|------|
| Design + spec | Days |
| SDK: override_channel parsing + validation | Days |
| SDK: invalidate+recompute template | 1-2 weeks |
| SDK: active section auto-generation | 1-2 weeks |
| Testing (unit + integration) | Days |
| Migration of cs-profiles | Hours (once SDK ships) |

## Sequence

1. **Now**: Ship Plan A-v2 quickwin (any_value/max merge on record-set inputs)
2. **Next**: Ship Plan A-v2 fewdays Phase 1 (hand-written active section tables for all CDC inputs)
3. **Then**: Build `override_channel` (Plan D) — auto-generates active section tables from input contracts
4. **Then**: Migrate cs-profiles from hand-written to auto-generated active section tables
5. **Then**: Build `lookback` key (Plan C) — same pattern for event streams
