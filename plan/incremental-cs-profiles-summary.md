# Incrementalizing cs-profiles: Summary for Engineering

**TL;DR**: In v0, we made cs-profiles incremental without touching `inputs.yaml` — everything that could be marked append-only was marked, and the rest was left for incremental SQL models. In this iteration, we dug deeper: opened up the upstream views, traced them to base tables, and found that most inputs are CDC tables where the same record appears across batches. Simple `merge: sum()` double-counts on these — only `any_value`/`max` merges are safe.

The correct approach for CDC inputs is **active section tables** — incremental SQL models that deduplicate each input to its current state before entity vars aggregate over it. Progress: 29 vars already incremental, 43 more in hours, 266 more in days (8 templated SQL models). A future SDK feature (`override_channel`) auto-generates these models from input contracts.

## Background

The original cs-profiles project consumed views — opaque SQL definitions managed outside our project. We had no visibility into how those views were constructed, what base tables they joined, whether the underlying data was append-only or mutable, or what the performance characteristics were. We accepted whatever the warehouse gave us and full-scanned everything on every run.

This investigation started by opening up those views and examining how they were created — tracing each view back to its base tables, checking DDLs, verifying append-only properties, and cataloging CDC columns (`_SDC_BATCHED_AT`, `UPDATEDAT`, `SYSTEMMODSTAMP`, `DELETED`, `_SDC_DELETED_AT`). The goal was to take ownership of the performance delivered by this project, rather than treating the upstream data layer as a black box.

The views investigated:

| View | What It Does | Base Tables | Vars |
|------|-------------|-------------|------|
| `UNION_UPDATED_MERGED_TICKETS` | Unions thena ticket creates + updates, deduplicates to latest state per ticket | `_CREATE` + `_UPDATE` (append-only) | 14 |
| `SALESFORCE_VIEW_TABLE` | Joins SF accounts with SF users to resolve TAM/CSM names | `SF_ACCOUNT` + `SF_USER` | 4 |
| `ACCOUNT_VIEW_TABLE` | Joins user accounts with plans to get org name and plan type | `USER_ACCOUNTS` + `PLANS` (both Stitch) | 8 |
| `GONG_CALLS` | Filters SF tasks to calls, deduplicates by call ID with regex extraction | `SF_TASK` (Salesforce sync) | 16 |
| `CUSTOMER_SUCCESS_CONNECTIONS` | 7-table join: connections × sources × destinations × definitions × transformations, filtered to active/enabled | 7 Stitch tables + 1 static mapping | 22 |
| `CUSTOMER_LIFETIME_MONTHLY_EVENT_VOLUME` | Passthrough to dbt-managed daily aggregate with SFDC data | `agg_event_volume_daily_with_sfdc` (dbt) | 13 |
| `CUSTOMER_LIFETIME_DAILY_EVENT_VOLUME` | Same dbt source, daily granularity | Same dbt model | 3 |

All views except the two dbt-backed ones resolve to Stitch or Salesforce base tables that are append-only at the storage layer. The data was already there for incremental processing — the views were just hiding it.

## The Two Kinds of Inputs

Tracing the views revealed that every input falls into one of two categories:

1. **Event streams** — each row is an independent event. Rows are never overridden. Examples: `app_tracks`, `app_identifies`.

2. **Record sets (CDC)** — each row represents the current state of a mutable record. Multiple rows exist for the same record (created, updated, deleted), distinguished by a primary key and ordered by a timestamp. Examples: `app_sources`, `sf_account`, all Stitch-synced tables.

Both are **append-only at the storage layer** — Stitch and Salesforce only INSERT, never UPDATE or DELETE warehouse rows. The difference is in business semantics. This distinction drives the entire incrementalization strategy.

## What's Done

### 1. Event stream incremental models (shipped)

Three incremental SQL models that materialize bounded time windows, refreshed incrementally:

| Model | Source | Pattern | Vars Served |
|-------|--------|---------|-------------|
| `recent_app_tracks` | `app_tracks` (append-only) | 90-day window: UNION(previous + delta), drop expired | 6 workspace vars |
| `recent_app_identifies` | `app_identifies` (append-only) | 90-day window: same pattern | 9 vars (3 workspace + 6 account) |
| `thena_current_state` | `thena_ticket_create` + `thena_ticket_update` (append-only) | CDC dedup: UNION(previous_state + delta), ROW_NUMBER by PK, keep latest | 14 account vars |

`thena_current_state` is particularly important — it's the **prototype for the active section table pattern** used throughout the rest of this plan.

**29 vars incremental after this step.**

### 2. Quick win: mark CDC base tables append-only (in progress, uncommitted)

Added `is_append_only: true` to 8 CDC inputs. This enables PB to generate delta tables (`incr_delta_tbl`) for each, so downstream entity vars with `merge:` clauses process only new rows.

Added `merge:` clauses to **43 vars** that use `any_value()` or `max()` — the only aggregations that are algebraically composable on CDC tables. `any_value(any_value(x), any_value(y)) = any_value(x, y)`.

Why only `any_value`/`max`? On CDC tables, the same record appears in multiple batches as it gets updated. `merge: sum()` on a `count(*)` would double-count — a source counted in batch 1 appears again in batch 2's delta, sum gives 2 instead of 1. This rules out `count`, `sum`, `count(DISTINCT)`, `avg` for direct merge on CDC inputs.

**~72 vars incremental after this step (29 existing + 43 new).**

## What's Planned

### Phase 1: Active section tables for all CDC inputs (days)

**The key decision**: every record-set (CDC) input should be consumed through an **active section table** — an incremental SQL model that deduplicates the raw input to its current state. Entity vars read from the active section table, never from the raw CDC input directly.

**Why**: On record-set inputs, any aggregation on the raw table produces wrong results when merged incrementally, because the same record contributes to multiple batches. The active section table deduplicates to exactly one row per record (latest state), making all aggregations correct — `count`, `sum`, `count(DISTINCT)`, `avg`, everything.

**Pattern** (same as `thena_current_state`):
```
First run:  dedup(full_input) by PK → materialize
Incremental: dedup(previous_state UNION delta) by PK → materialize
```

**8 active section tables to create**, one per CDC input:

| Model | Raw Input | PK | Ordering | Downstream Vars |
|-------|-----------|----|---------|-|
| `sources_current_state` | `app_sources` | `ID` | `UPDATEDAT` | 6 |
| `destinations_current_state` | `app_destinations` | `ID` | `UPDATEDAT` | 5 |
| `transformations_current_state` | `app_transformations` | `ID` | `UPDATEDAT` | 6 |
| `workspaces_current_state` | `app_workspaces` | `ID` | `_SDC_BATCHED_AT` | 15 |
| `user_roles_current_state` | `app_user_roles` | `ID` | `UPDATEDAT` | 2 |
| `tracking_plans_current_state` | `app_tracking_plans` | `ID` | `UPDATEDAT` | 1 |
| `profiles_current_state` | `app_profiles` | `ID` | `_SDC_BATCHED_AT` | 1 |
| `sf_account_current_state` | `sf_account` | `ID` | `SYSTEMMODSTAMP` | 35 |

Entity vars are repointed from `inputs/X` to `models/X_current_state`. No new entity types, id stitchers, or rollup models needed.

**~338 vars incremental after this step** (including 238 rollup vars that inherit from now-incremental workspace vars).

### Phase 2: Decompose mutable views (days)

4 inputs read from mutable views (joins over base tables). These can't be marked append-only. Decompose each view to its base tables, add them as inputs with active section tables.

| View | Base Tables | Vars | Complexity |
|------|------------|------|-----------|
| `sf_account_view` (TAM/CSM names) | `SF_ACCOUNT` + `SF_USER` | 4 | Easy |
| `app_user_accounts` (org/plan info) | `USER_ACCOUNTS` + `PLANS` | 8 | Easy |
| `gong_call` (call metrics) | `SF_TASK` | 16 | Moderate |
| `app_cs_connections` (connection counts) | 7 Stitch tables | 22 | Moderate |

**~388 vars incremental after this step.**

### Remaining full-scan (16 vars)

16 vars from `monthly_event_volume` and `daily_event_volume` — backed by a dbt model rebuilt by an external pipeline. Not incrementalizable from our side.

### PB SDK: `override_channel` (weeks, future)

The active section tables from Phase 1 are hand-written. A PB SDK feature (`override_channel` on input contracts) would auto-generate them:

```yaml
# Proposed — tells PB this input has record-set semantics
contract:
  is_append_only: true
  override_channel: ID           # PK — rows with same ID override each other
  override_ordering: UPDATEDAT   # latest wins
```

PB would then:
- Auto-generate the active section table (dedup by PK, keep latest)
- For unbounded vars: invalidate + recompute for affected entities only
- For time-windowed vars: maintain time-bounded active section
- Entity vars don't need `merge:` — PB infers the correct strategy from the input contract

This eliminates all hand-written SQL models for CDC inputs. Design is documented in `plan-d-sdk-cdc-merge.md`.

## Scorecard

| Metric | Before | Quick Win | + Phase 1 | + Phase 2 | SDK (`override_channel`) |
|--------|--------|-----------|-----------|-----------|--------------------------|
| Inputs with `is_append_only` | 4 | 12 | 12 | ~16 | ~16 |
| Active section tables | 1 | 1 | 9 | ~13 | Auto-generated |
| Vars incremental | 29 | ~72 | ~338 | ~388 | ~391 |
| Vars full-scan | ~378 | ~335 | ~69 | ~19 | 16 |
| New PB SDK features | — | None | None | None | `override_channel` |
| Hand-written SQL models | 3 | 3 | 11 | ~15 | 0 (auto-generated) |

## Architecture

```
                         Event streams                    Record sets (CDC)
                              │                                  │
                    ┌─────────┴──────────┐            ┌──────────┴───────────┐
                    │                    │            │                      │
              Bounded window      Direct merge    Active section table    Direct merge
              SQL models          (merge: on      (dedup by PK,          (merge: on
              (recent_app_*)      entity var)      keep latest)           any_value/max)
                    │                    │            │                      │
                    └─────────┬──────────┘            └──────────┬───────────┘
                              │                                  │
                              ▼                                  ▼
                    Workspace entity vars ──────────→ workspace_profile (VIEW)
                                                              │
                                                      rolledUpWorkspaces
                                                              │
                                                              ▼
                    Account entity vars ◄──────────── rollup vars
                              │
                              ▼
                    account_profile (VIEW)
                              │
                       ┌──────┴──────┐
                 AllCustomers    CurrentCustomers
                 Prospects       (cohort)
```

## Key Insight: Event Streams vs Record Sets

This is the fundamental taxonomy. Every incrementalization decision flows from it:

| | Event Stream | Record Set |
|---|---|---|
| **Each row is** | An independent event | A state change to a mutable record |
| **`count(*)` means** | How many events happened | How many records currently exist |
| **Safe merge aggregations** | All (`sum`, `count`, `min`, `max`, `avg`) | Only `any_value`, `max` |
| **Incrementalization strategy** | Algebraic merge (merge expression on entity var) | Active section table (dedup to current state, then aggregate) |
| **PB SDK (future)** | `is_event_stream: true` | `override_channel: <PK>` |

Industry parallel: ksqlDB's STREAM vs TABLE, Flink's append vs changelog streams, dbt's `unique_key` on incremental models.

## Files Changed / To Change

| File | Quick Win (done) | Phase 1 | Phase 2 |
|------|-----------------|---------|---------|
| `models/inputs.yaml` | +8 `is_append_only` | No change | +new base-table inputs |
| `models/profiles.yaml` | +43 `merge:` clauses | Repoint vars to active section tables, remove `merge:` | Rewrite view-sourced vars |
| `models/sql_models.yaml` | No change | +8 active section models | +active section models for decomposed views |
| `pb_project.yaml` | No change | No change | No change |

## Effort

| Phase | Work | Time | PB SDK Changes |
|-------|------|------|----------------|
| Quick win | `is_append_only` flags + `merge:` clauses | Hours | None |
| Phase 1 | 8 incremental SQL models, repoint ~74 vars | Days | None |
| Phase 2 | Decompose 4 views, new inputs + active section tables | Days | None |
| SDK: `override_channel` | Parse contract, auto-generate active section tables | Weeks | Yes |

## Related Documents

| Document | Contents |
|----------|----------|
| `plan-a-v2-quickwin.md` | Detailed analysis: which vars benefit from `is_append_only` + `merge:`, which don't and why |
| `plan-a-v2-fewdays-win.md` | Phase 1-3 detailed plan: active section tables, view decomposition, cross-entity analysis |
| `plan-d-sdk-cdc-merge.md` | PB SDK proposal: `override_channel`, invalidate+recompute, auto-generated active section tables |
| `plan-a-quickwin-bounded-window.md` | Original plan A (bounded window models — implemented) |
| `plan-b-tumbling-window-incremental-sql.md` | Plan B (tumbling windows — subsumed by active section tables) |
| `plan-c-entity-var-lookback-key.md` | Plan C (`lookback:` key on entity vars — complementary, for event streams) |
