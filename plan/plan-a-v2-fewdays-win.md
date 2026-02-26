# Plan A v2: Few-Days Win — Active Section Tables + View Decomposition

**Prerequisite**: Complete `plan-a-v2-quickwin.md` first (mark 8 base tables append-only).

After the quick win, 12 of 19 inputs are append-only and ~72 of 407 vars are incremental (29 existing + 43 `any_value`/`max` vars with merge). The remaining non-incremental vars fall into four buckets:

1. **`count(*)` / `count(ID)` on CDC tables** (28 vars) — same entity appears in multiple batches, `merge: sum()` double-counts
2. **`count(DISTINCT)` on CDC tables** (3 vars) — same double-counting issue
3. **Vars reading from mutable views** (50 vars) — views need decomposition to base tables
4. **Rollup vars** (238 vars) — inherit incrementality from upstream workspace vars
5. **dbt-backed event volume** (16 vars) — not incrementalizable

This plan addresses buckets 1–3, which unlocks bucket 4.

## Key Decision: Active Section Tables for All Record-Set Inputs

Every CDC/record-set input should always be consumed through an **active section table** — an incremental SQL model that deduplicates the raw input to its current state. Entity vars read from the active section table, never from the raw CDC input.

**Why**: On record-set inputs, the raw table contains multiple rows per record (created, updated, deleted). Any aggregation (`count`, `sum`, `count(DISTINCT)`, `avg`, `min`, `max`) on the raw table produces wrong results when merged incrementally, because the same record contributes to multiple batches. Only `any_value` is safe (and even that returns an arbitrary historical value, not the latest).

**Active section table pattern**:
1. On first run: dedup raw input by PK, keep latest row per record → materialize
2. On incremental run: UNION previous active section + delta rows → dedup again → materialize
3. Entity vars read from this deduplicated table — simple aggregation, no merge complexity
4. `count(ID) WHERE DELETED='FALSE'` is correct because dedup ensures each record appears exactly once

The `thena_current_state` model already implements this pattern. Phase 1 generalizes it to all 8 CDC inputs.

When PB SDK ships `override_channel` (see `plan-d-sdk-cdc-merge.md`), these hand-written SQL models become auto-generated from input contracts.

## How to Verify

Same sources as `plan-a-v2-quickwin.md`:
- **Project files**: `made_incremental/models/`
- **Warehouse results**: `made_incremental/results` (DDLs, column listings)
- **View DDLs**: All 6 verified in `results` — see Appendix below

---

## Phase 1: Active Section Tables for All CDC Inputs

### Problem

Two categories of vars can't use simple merge on CDC tables:

1. **28 `count(*)` / `count(ID)` vars**: The same record (source, destination, workspace, etc.) appears in multiple batches as it gets updated. `merge: sum()` double-counts — e.g., a source counted as 1 in batch 1 appears again in batch 2's delta, sum gives 2 instead of 1.

2. **3 `count(DISTINCT X)` vars**: Same double-counting issue. Additionally, `sum(count_distinct_batch1, count_distinct_batch2)` overcounts when the same value appears in both batches.

3. **43 `any_value` vars** (already incremental from quickwin): Technically safe, but return an arbitrary historical value, not the latest. Active section tables give correct latest-value semantics for free.

### Solution: One Active Section Table Per CDC Input

For each of the 8 CDC inputs, create an incremental SQL model that deduplicates to current state. This is the same pattern as the existing `thena_current_state` model.

### Pattern

```yaml
# In sql_models.yaml — active section table for app_sources
- name: sources_current_state
  model_type: sql_template
  model_spec:
    materialization:
      output_type: table
    single_sql: |
      {%- set lastThis = this.DeRef(pre_existing=true, dependency="optional", checkpoint_name="baseline") -%}
      {%- set deltaInput = this.DeRef("inputs/app_sources/incr_delta") -%}
      {%- set fullInput = this.DeRef("inputs/app_sources") -%}
      {% if lastThis is none %}
        -- First run: dedup the full input
        SELECT * FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ID ORDER BY UPDATEDAT DESC
          ) AS _rn FROM {{fullInput}}
        ) WHERE _rn = 1
      {% else %}
        -- Incremental: union previous state + delta, dedup
        SELECT * FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ID ORDER BY UPDATEDAT DESC
          ) AS _rn FROM (
            SELECT * FROM {{lastThis}}
            UNION ALL
            SELECT * FROM {{deltaInput}}
          )
        ) WHERE _rn = 1
      {% endif %}
    ids:
      - select: "WORKSPACEID"
        type: workspace_id
        entity: workspace
```

Entity vars then read from the active section table:

```yaml
# In profiles.yaml — vars read from active section, not raw input
- entity_var:
    name: app_number_sources_created
    select: count(ID)
    from: models/sources_current_state         # was: inputs/app_sources
    where: DELETED = 'FALSE'
    # No merge: needed — active section is already deduplicated
    # On incremental run, PB full-scans this (small) table per entity
```

### Active Section Tables to Create

| Model Name | Raw Input | PK Column | Ordering Column | Downstream Vars |
|------------|-----------|-----------|-----------------|-----------------|
| `sources_current_state` | `app_sources` | `ID` | `UPDATEDAT` | 6 workspace vars |
| `destinations_current_state` | `app_destinations` | `ID` | `UPDATEDAT` | 5 workspace vars |
| `transformations_current_state` | `app_transformations` | `ID` | `UPDATEDAT` | 6 workspace vars |
| `workspaces_current_state` | `app_workspaces` | `ID` | `_SDC_BATCHED_AT` | 15 workspace vars |
| `user_roles_current_state` | `app_user_roles` | `ID` | `UPDATEDAT` | 2 account vars |
| `tracking_plans_current_state` | `app_tracking_plans` | `ID` | `UPDATEDAT` | 1 workspace var |
| `profiles_current_state` | `app_profiles` | `ID` | `_SDC_BATCHED_AT` | 1 workspace var |
| `sf_account_current_state` | `sf_account` | `ID` | `SYSTEMMODSTAMP` | 35 account vars |
| *(existing)* `thena_current_state` | `thena_ticket_create` + `thena_ticket_update` | `REQUEST_ID\|\|TEAM_ID` | `UPDATED_AT` | 14 account vars |

### What Changes Per Input

Per CDC input (much simpler than the old CDC entity approach — no new entity types, id stitchers, or rollup models):
1. New incremental SQL model in `sql_models.yaml` (dedup to current state)
2. Entity vars repointed: `from: inputs/X` → `from: models/X_current_state`
3. Remove `merge:` clauses from repointed vars (active section is already deduplicated; PB aggregates over the full materialized table)

### Why This Is Better Than CDC Entities

The previous approach (CDC entities + rollup models) required per-input:
- New entity type in `pb_project.yaml`
- New id type in `pb_project.yaml`
- Id stitcher (trivial but boilerplate)
- Entity var group with `MAX_BY` vars + `merge:`
- Feature table model (VIEW)
- Rollup SQL model

Active section tables require only:
- One incremental SQL model (dedup template)
- Repoint existing entity vars

No new entity types, no id stitchers, no rollup models, no `MAX_BY` vars. The dedup happens once in SQL, and downstream entity vars work exactly as they do today — just reading from deduplicated data.

### Impact

- **43 `any_value`/`max` vars**: Repointed to active section tables. Remove quickwin `merge:` clauses — vars now read deduplicated data directly, getting latest value (better than `any_value` which returns arbitrary row)
- **28 `count` vars**: Now correct — dedup ensures each record counted once. `count(ID) WHERE DELETED='FALSE'` works because only the latest row per record is present
- **3 `count(DISTINCT)` vars**: Now correct — no HLL needed, dedup ensures each record appears once
- **238 rollup vars**: Become incremental (inherit from now-incremental workspace vars)
- **Total after Phase 1**: ~338 of 407 vars incremental

### Note on `merge:` After Phase 1

Once entity vars read from active section tables (which are regular materialized tables, not append-only inputs), they don't need `merge:` clauses. PB full-scans the active section table for each entity. The active section table itself is incrementally maintained (small: only delta + previous state), so the full scan is cheap.

The quickwin `merge:` clauses on `any_value`/`max` vars from `inputs/sf_account` and `inputs/app_workspaces` should be **removed** when those vars are repointed to active section tables. Merge is only useful when reading directly from an append-only input.

---

## Phase 2: Decompose Mutable Views

### Problem

50 vars read from 4 mutable views. These views are convenience layers over Stitch/Salesforce base tables. By reading from the views, PB must full-scan on every run.

### Verified View DDLs

All DDLs retrieved from warehouse — see `made_incremental/results`.

**1. SALESFORCE_VIEW_TABLE** (`sf_account_view`) — 4 vars, Easy

```sql
SELECT A.ID as ACCOUNT_ID, A.NAME as ACCOUNT_NAME,
       B.NAME as CSM_NAME, C.NAME as TAM_NAME, A.record_type_id, A.CREATED_DATE
FROM RUDDER_SALESFORCE.SF_ACCOUNT AS A
INNER JOIN RUDDER_SALESFORCE.SF_USER AS B ON A.ASSIGNED_CSM_C = B.ID
INNER JOIN RUDDER_SALESFORCE.SF_USER AS C ON A.ASSIGNED_TAM_C = C.ID
```

- 2 base tables: `SF_ACCOUNT` (already an input), `SF_USER` (new)
- Only 4 vars: `sf_tam`, `sf_csm` (×2 cohorts)
- **Action**: Add `SF_USER` as append-only input with active section table. Rewrite `sf_tam`/`sf_csm` to join `sf_account_current_state` (Phase 1) with `sf_user_current_state` on `ASSIGNED_TAM_C`/`ASSIGNED_CSM_C`.

**2. ACCOUNT_VIEW_TABLE** (`app_user_accounts`) — 8 vars, Easy

```sql
SELECT ORGID, ORGNAME, PLANID, DISPLAYNAME, A.CREATEDAT
FROM PPW_PUBLIC.USER_ACCOUNTS AS A
LEFT JOIN PPW_PUBLIC.PLANS AS B ON A.PLANID = B.ID
```

- 2 base tables: `USER_ACCOUNTS`, `PLANS` (both Stitch, `_SDC_BATCHED_AT`)
- 8 vars: `app_org_name`, `app_org_id`, `app_plan_type`, `app_plan_type` (×2 cohorts + basic)
- **Action**: Add both as append-only inputs with active section tables. Entity vars read from `user_accounts_current_state` joined with `plans_current_state`.

**3. GONG_CALLS** (`gong_call`) — 16 vars, Moderate

```sql
SELECT ANY_VALUE(ACTIVITY_DATE) as DATE, ANY_VALUE(ACCOUNT_ID) as SALESFORCE_ID,
       ANY_VALUE(CALL_DURATION_IN_SECONDS) as DURATION,
       CASE WHEN CALL_OBJECT IS NULL
            THEN regexp_substr(DESCRIPTION, $$\?id=(.*)\s$$,1,1,'e')
            ELSE CALL_OBJECT END as CALL_ID
FROM RUDDER_SALESFORCE.SF_TASK
WHERE TYPE = 'Call' AND ACCOUNT_ID IS NOT NULL AND CALL_DURATION_IN_SECONDS IS NOT NULL
GROUP BY CALL_ID ORDER BY DATE DESC
```

- 1 base table: `SF_TASK` (Salesforce sync, `SYSTEMMODSTAMP`)
- 16 vars: `sf_call_count_last_30/60/90`, `sf_call_duration_last_30/60/90`, pre/post sale variants (×2 cohorts)
- **Action**: Add `SF_TASK` as append-only input with active section table. The active section table replicates the GROUP BY call_id + regex dedup, incrementally. Time-windowed count/sum vars read from `gong_calls_current_state`.

**4. CUSTOMER_SUCCESS_CONNECTIONS** (`app_cs_connections`) — 22 vars, Moderate

```sql
FROM PPW_PUBLIC.CONNECTIONS A
LEFT JOIN PPW_PUBLIC.SOURCES B ON A.SOURCEID = B.ID
LEFT JOIN PPW_PUBLIC.DESTINATIONS C ON A.DESTINATIONID = C.ID
LEFT JOIN PPW_PUBLIC.SOURCE_DEFINITIONS D ON B.SOURCEDEFINITIONID = D.ID
LEFT JOIN (PPW_PUBLIC.DESTINATION_DEFINITIONS x
           LEFT JOIN CUSTOMER_SUCCESS.PUBLIC.DESTINATION_CATEGORY_MAPPING y ON x.ID = y.ID) E
     ON C.DESTINATIONDEFINITIONID = E.ID
LEFT JOIN PPW_PUBLIC.DESTINATION_TRANSFORMATIONS F ON A.DESTINATIONID = F.DESTINATIONID
LEFT JOIN PPW_PUBLIC.TRANSFORMATIONS G ON F.TRANSFORMATIONID = G.ID
WHERE A.DELETED = FALSE AND A.ENABLED = TRUE
  AND B.ENABLED = TRUE AND B.DELETED = FALSE
  AND C.ENABLED = TRUE AND C.DELETED = FALSE
```

- 7 base tables (all Stitch, `_SDC_BATCHED_AT`) + 1 static mapping
- 22 vars: `count(DISTINCT SOURCEID/DESTINATIONID/TRANSFORMATIONID) WHERE category = X`
- **Action**: Two options:
  - (a) Add `CONNECTIONS`, `SOURCE_DEFINITIONS`, `DESTINATION_DEFINITIONS`, `DESTINATION_TRANSFORMATIONS` as new append-only inputs, each with active section tables. Create a `connections_current_state` model that joins the deduplicated tables. Complex but fully incremental.
  - (b) Create a single incremental SQL model that unions previous state + delta from all 7 base tables, replicates the join + dedup logic. Simpler but hand-written.
  - (c) Leave it as-is (full-scan view) and accept 22 vars as non-incremental.

### Recommended Order

1. `sf_account_view` (4 vars, easy, `SF_USER` is only new input)
2. `app_user_accounts` (8 vars, easy, 2 new inputs)
3. `gong_call` (16 vars, moderate, 1 new input)
4. `app_cs_connections` (22 vars, moderate, 4 new inputs)

### Impact

- **50 view-sourced vars** become incremental
- Combined with Phase 1: **~388 of 407 vars incremental**
- Remaining 16 full-scan: event volume (dbt) + 3 derived (always recomputed, no table scan)

---

## Phase 3: Unify All Intermediate Models Under Active Section Pattern

After Phase 1, the project will have ~9 active section tables (8 new + `thena_current_state`). Phase 3 standardizes and optionally extends the pattern:

1. **`thena_current_state`** is already an active section table — it was the prototype. No changes needed.

2. **`recent_app_tracks`** and **`recent_app_identifies`** are **time-bounded active section tables** for event streams. They implement the same UNION + dedup pattern, just without override (since event streams have no PK to dedup by) and with a time window bound. These stay as-is.

3. **Time-bounded active section tables for CDC inputs**: If any CDC-sourced vars use time-windowed WHERE clauses (e.g., `sources_created_last_90`), the active section table can optionally include a time bound to limit materialization size. For now, unbounded dedup (full current state) is simpler and sufficient — CDC tables are small relative to event streams.

When PB SDK ships `override_channel` (Plan D), all hand-written active section tables become auto-generated from input contracts.

---

## Scorecard

| Metric | After Quick Win | After Phase 1 | After Phase 2 | After Phase 3 |
|--------|----------------|---------------|---------------|---------------|
| Inputs with `is_append_only` | 12 | 12 | ~16 | ~16 |
| Views consumed | 6 | 6 | 2 (event volume only) | 2 |
| Active section tables | 1 (thena) | 9 | ~13 | ~13 |
| Vars incremental | ~72 | ~338 | ~388 | ~391 |
| Vars full-scan | ~335 | ~69 | ~19 | 16 (event volume) |
| New entity types | 0 | 0 | 0 | 0 |
| PB SDK changes | None | None | None | None |

## Effort

| Phase | Work | Time |
|-------|------|------|
| Phase 1: Active section tables | 8 incremental SQL models (templated), repoint ~74 entity vars | Days |
| Phase 2: Decompose views | New inputs, active section tables for decomposed base tables, rewrite vars | Days |
| Phase 3: Unify pattern | Standardize, optional time-bounding | Hours |

## Files to Modify

| File | Phase 1 | Phase 2 | Phase 3 |
|------|---------|---------|---------|
| `pb_project.yaml` | No change | No change | No change |
| `models/inputs.yaml` | No change (already append-only from quickwin) | Add new base-table inputs | No change |
| `models/profiles.yaml` | Repoint vars to active section tables, remove `merge:` clauses | Rewrite view-sourced vars | No change |
| `models/sql_models.yaml` | Add 8 active section SQL models | Add active section models for decomposed views | No change |

## Verification

Same as quick win plan:
1. Full refresh original → baseline
2. Full refresh incremental → compare (`FULL OUTER JOIN`, zero diffs)
3. Incremental run → compare again
4. Measure wall-clock time

---

## Analysis: Cross-Entity Relationships

### Do any entities interact in a way that requires aggregating their relationships?

Phase 1 active section tables (`sources_current_state`, `destinations_current_state`, etc.) are all **independent**. Each deduplicates a single base table. Entity vars read from these tables and aggregate to workspace or account via a single foreign key. No active section table needs data from another.

**Exception: `CUSTOMER_SUCCESS_CONNECTIONS` (Phase 2)**

The 22 `app_cs_connections` vars are different. They do things like:

```sql
count(DISTINCT SOURCEID) WHERE DESTINATION_CS_CATEGORY = 'Marketing'
count(DISTINCT DESTINATIONID) WHERE SOURCE_TYPE = 'Cloud'
```

A **connection** is inherently a relationship between a source and a destination. The vars need properties from multiple joined entities:

```
CONNECTIONS → SOURCES → SOURCE_DEFINITIONS
CONNECTIONS → DESTINATIONS → DESTINATION_DEFINITIONS → DESTINATION_CATEGORY_MAPPING
CONNECTIONS → DESTINATION_TRANSFORMATIONS → TRANSFORMATIONS
```

This means a simple single-table active section table doesn't cleanly apply. A `connections_current_state` model would need to join deduplicated state from 7 tables, and the "last updated" timestamp would need to be `GREATEST()` across all tables — if any related record changes, the connection's joined state changes too.

This is an open design question. Options include:
- **(a)** Individual active section tables per base table, joined in a composite `connections_current_state` model — fully incremental but complex
- **(b)** A single incremental SQL model that unions previous state + delta, replicates the join + dedup logic — simpler but hand-written
- **(c)** Leave it as-is (full-scan view) and accept 22 vars as non-incremental

No decision made yet. This should be revisited once the simpler Phase 1 entities are proven.

---

## Appendix: View Investigation (Verified)

All 6 view DDLs retrieved from warehouse. Full DDLs in `made_incremental/results`.

### Confidence Summary

| View Input | Base Tables | Append-Only? | Confidence |
|------------|------------|--------------|------------|
| `sf_account_view` | `SF_ACCOUNT` + `SF_USER` | Yes (Salesforce sync) | **Verified** |
| `app_user_accounts` | `USER_ACCOUNTS` + `PLANS` | Yes (both Stitch, `_SDC_BATCHED_AT`) | **Verified** |
| `gong_call` | `SF_TASK` | Yes (Salesforce sync, `SYSTEMMODSTAMP`) | **Verified** |
| `app_cs_connections` | `CONNECTIONS`, `SOURCES`, `DESTINATIONS`, `SOURCE_DEFINITIONS`, `DESTINATION_DEFINITIONS`, `DESTINATION_TRANSFORMATIONS`, `TRANSFORMATIONS` + `DESTINATION_CATEGORY_MAPPING` | Yes (all Stitch) | **Verified** (full DDL) |
| `monthly_event_volume` | `ANALYTICS_DB.AGG_EVENT_VOLUME_DAILY_WITH_SFDC` | **No** — dbt model | **Verified** (not incrementalizable) |
| `daily_event_volume` | Same | **No** — dbt model | **Verified** (not incrementalizable) |

### dbt Model Detail

Both event volume views read from `ANALYTICS_DB.PUBLIC.AGG_EVENT_VOLUME_DAILY_WITH_SFDC`, which is:
```sql
SELECT * FROM ANALYTICS.analytics.agg_event_volume_daily_with_sfdc
-- dbt model: model.dbt_reports_prod.agg_event_volume_daily_with_sfdc
```
Rebuilt by dbt. No `_SDC_BATCHED_AT` or `UPDATEDAT`. 16 vars stay full-scan.
