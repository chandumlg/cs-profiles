# Plan B: Tumbling Window Incremental SQL Models

## Concept

**Tumbling windows** = maintain pre-aggregated daily (or monthly) materializations, then assemble rolling windows by unioning N daily slices.

Two-layer architecture:
1. **Daily aggregation model** — one materialization per day, pre-aggregated (GROUP BY entity_id + dimension columns, SUM/COUNT metrics). Marked `time_grain: day`, `is_append_only: true`. Only the latest day's data is processed on each run.
2. **Rolling window model** — uses `MultiDeRef` to union the last N daily materializations. Output is ephemeral (computed at query time, not stored). Entity vars read from this.

### Why This Is Better Than Plan A

| Dimension | Plan A (bounded window) | Plan B (tumbling) |
|-----------|------------------------|-------------------|
| Daily incremental work | Re-scans full 90-day raw events | Only aggregates today's new events |
| Entity var scan size | Full 90-day event table | 90 small pre-aggregated daily tables |
| Pre-aggregation | None — entity var does GROUP BY over raw events | Daily model pre-aggregates; rolling model re-aggregates small summaries |
| True incrementality | Only for append-only inputs (tracks/identifies) | For ALL inputs — daily model always processes just one day |

The key insight: even for mutable views like `gong_call`, the daily aggregation model processes one day's worth of data. And entity vars read from pre-aggregated summaries, not raw events.

## Reference Implementation (from spigot project)

The spigot project (commit `6aec6c8`) demonstrates this pattern:

```yaml
# Layer 1: Daily aggregation (one materialization per day)
- name: searches_daily_category_agg
  model_type: sql_template
  model_spec:
    time_grain: day
    occurred_at_col: event_date
    single_sql: |
      SELECT main_id, tier_1_name, SUM(searches) as daily_searches
      FROM {{this.DeRef('inputs/wave_searches_categories')}} w
      JOIN {{this.DeRef('models/wave_users')}} m ON w.user_id = m.user_id
      GROUP BY main_id, tier_1_name
    materialization:
      output_type: table
    contract:
      is_append_only: true

# Layer 2: Rolling 7-day window (union of 7 daily materializations)
- name: searches_7d_agg
  model_type: sql_template
  model_spec:
    single_sql: |
      {% set rangeStart = this.GetWhtContext().TimeInfo.Subtract("168h") %}
      {% set rangeEnd = this.GetWhtContext().TimeInfo %}
      {% set pastMats = this.MultiDeRef('models/searches_daily_category_agg',
          pre_existing=true, range_start=rangeStart, range_end=rangeEnd) %}
      {% set idDelta = this.DeRef('models/wave_users/id_cluster_delta',
          dependency="optional", pre_existing=true) %}
      SELECT COALESCE(d.new_main_id, a.main_id) as main_id,
             a.tier_1_name,
             SUM(a.daily_searches) as daily_searches
      FROM (
          {% for mat in pastMats %}
              {% if not loop.first %}UNION ALL{% endif %}
              SELECT main_id, tier_1_name, daily_searches FROM {{mat}}
          {% endfor %}
      ) a
      {% if idDelta.IsEnabled() %}
      LEFT JOIN {{idDelta}} d ON a.main_id = d.old_main_id
      {% else %}
      LEFT JOIN (SELECT NULL as old_main_id, NULL as new_main_id WHERE 1=0) d
        ON a.main_id = d.old_main_id
      {% endif %}
      GROUP BY 1, 2
    materialization:
      output_type: ephemeral

# Entity var reads from rolling window — no time filter needed
- entity_var:
    name: technology_7d_searches
    select: sum(daily_searches)
    from: models/searches_7d_agg
    where: tier_1_name = 'technology & computing'
```

## Design Questions / Open Issues

### 1. Does `time_grain: day` + `MultiDeRef` actually work in PB today?

The spigot implementation uses:
- `time_grain: day` on the daily agg model
- `this.MultiDeRef(model, pre_existing=true, range_start=..., range_end=...)` in the rolling window model
- `this.GetWhtContext().TimeInfo.Subtract("168h")` for range calculation

**Need to verify**: Are `MultiDeRef`, `time_grain`, and `GetWhtContext().TimeInfo.Subtract()` supported in current PB? The spigot commit exists, suggesting they are at least prototyped. But are they production-ready?

### 2. How does `time_grain: day` materialize?

Does PB create one table per day (e.g., `model_name_2026_02_26`)? Or one table with a partition column? This affects:
- Storage overhead (90 separate tables vs 1 partitioned table)
- Cleanup of old daily materializations (need to drop tables older than the max window)
- How `MultiDeRef` resolves range queries

### 3. ID cluster delta handling

The spigot pattern includes `id_cluster_delta` LEFT JOIN to handle ID graph changes (when entity IDs merge). cs-profiles uses `account` and `workspace` entities with id stitching. The daily agg would use the entity's main_id at the time of aggregation; the rolling window would need to re-key via `id_cluster_delta`.

### 4. Pre-aggregation column design

For each input, we need to decide what to pre-aggregate in the daily model. The daily model must include all dimension columns that entity vars filter on.

## Proposed Models for cs-profiles

### Layer 1: Daily Aggregations

#### `gong_calls_daily_agg`

Pre-aggregates gong calls per account per day.

```yaml
- name: gong_calls_daily_agg
  model_type: sql_template
  model_spec:
    time_grain: day
    occurred_at_col: DATE
    single_sql: |
      SELECT SALESFORCE_ID,
             CAST(DATE AS DATE) as call_date,
             COUNT(*) as daily_call_count,
             SUM(DURATION) as daily_duration
      FROM {{ this.DeRef('inputs/gong_call') }}
      GROUP BY SALESFORCE_ID, CAST(DATE AS DATE)
    materialization:
      output_type: table
    contract:
      is_append_only: true
    ids:
      - select: SALESFORCE_ID
        type: salesforce_id
        entity: account
```

#### `daily_event_volume_daily_agg`

Already daily-granularity data, so the "daily agg" is essentially a passthrough with entity ID resolution.

```yaml
- name: daily_event_volume_daily_agg
  model_type: sql_template
  model_spec:
    time_grain: day
    occurred_at_col: REPORT_DATE
    single_sql: |
      SELECT WORKSPACEID, REPORT_DATE, DAILY_TOTAL
      FROM {{ this.DeRef('inputs/daily_event_volume') }}
    materialization:
      output_type: table
    contract:
      is_append_only: true
    ids:
      - select: WORKSPACEID
        type: workspace_id
        entity: workspace
```

#### `monthly_event_volume_monthly_agg`

Monthly granularity — use `time_grain: month` if supported, or just `time_grain: day` with monthly data.

```yaml
- name: monthly_event_volume_monthly_agg
  model_type: sql_template
  model_spec:
    time_grain: month   # or day if month not supported
    occurred_at_col: REPORT_DATE
    single_sql: |
      SELECT WORKSPACEID, REPORT_DATE, MONTHLY_TOTAL
      FROM {{ this.DeRef('inputs/monthly_event_volume') }}
    materialization:
      output_type: table
    contract:
      is_append_only: true
    ids:
      - select: WORKSPACEID
        type: workspace_id
        entity: workspace
```

#### `app_tracks_daily_agg`

Replaces `recent_app_tracks`. Pre-aggregates tracks events per workspace per event type per day.

```yaml
- name: app_tracks_daily_agg
  model_type: sql_template
  model_spec:
    time_grain: day
    occurred_at_col: ORIGINAL_TIMESTAMP
    single_sql: |
      SELECT CONTEXT_TRAITS_WORKSPACE_ID,
             EVENT_TEXT,
             CAST(ORIGINAL_TIMESTAMP AS DATE) as event_date,
             COUNT(*) as daily_count
      FROM {{ this.DeRef('inputs/app_tracks') }}
      GROUP BY CONTEXT_TRAITS_WORKSPACE_ID, EVENT_TEXT, CAST(ORIGINAL_TIMESTAMP AS DATE)
    materialization:
      output_type: table
    contract:
      is_append_only: true
    ids:
      - select: CONTEXT_TRAITS_WORKSPACE_ID
        type: workspace_id
        entity: workspace
```

#### `app_identifies_daily_agg`

Replaces `recent_app_identifies`. **Problem**: the downstream vars use `count(DISTINCT USER_ID)`. Pre-aggregating to daily counts loses the ability to dedup users across days. Options:
- (a) Store daily distinct user lists (not aggregated) — defeats the purpose of pre-aggregation
- (b) Accept approximate: daily distinct counts summed over days overcounts users active on multiple days
- (c) Use HyperLogLog / approximate count distinct if warehouse supports it

**This is a fundamental limitation of tumbling windows for COUNT(DISTINCT) aggregations.** The `recent_app_identifies` bounded-window approach (Plan A) is actually correct here because it preserves row-level data.

**Decision**: Keep `recent_app_identifies` as-is (Plan A approach) for DISTINCT-based vars. Tumbling windows only work for SUM/COUNT/MIN/MAX-style aggregations.

### Layer 2: Rolling Window Models

#### `gong_calls_90d` / `gong_calls_60d` / `gong_calls_30d`

```yaml
- name: gong_calls_90d
  model_type: sql_template
  model_spec:
    single_sql: |
      {% set rangeStart = this.GetWhtContext().TimeInfo.Subtract("2160h") %}
      {% set rangeEnd = this.GetWhtContext().TimeInfo %}
      {% set pastMats = this.MultiDeRef('models/gong_calls_daily_agg',
          pre_existing=true, range_start=rangeStart, range_end=rangeEnd) %}
      {% set idDelta = this.DeRef('models/account_id_graph/id_cluster_delta',
          dependency="optional", pre_existing=true) %}
      SELECT COALESCE(d.new_main_id, a.SALESFORCE_ID) as SALESFORCE_ID,
             SUM(a.daily_call_count) as total_calls,
             SUM(a.daily_duration) as total_duration
      FROM (
          {% for mat in pastMats %}
              {% if not loop.first %}UNION ALL{% endif %}
              SELECT SALESFORCE_ID, daily_call_count, daily_duration FROM {{mat}}
          {% endfor %}
      ) a
      {% if idDelta.IsEnabled() %}
      LEFT JOIN {{idDelta}} d ON a.SALESFORCE_ID = d.old_main_id
      {% else %}
      LEFT JOIN (SELECT NULL as old_main_id, NULL as new_main_id WHERE 1=0) d
        ON a.SALESFORCE_ID = d.old_main_id
      {% endif %}
      GROUP BY 1
    ids:
      - select: SALESFORCE_ID
        type: salesforce_id
        entity: account
    materialization:
      output_type: ephemeral
```

Entity vars then become:
```yaml
- entity_var:
    name: sf_call_count_last_90
    select: any_value(total_calls)
    from: models/gong_calls_90d
```

**Alternatively**: Create a single `gong_calls_max_window` (90d) and have vars with shorter windows still filter by date. But that requires keeping the date column in the daily agg (un-aggregated by date), which partially defeats the purpose.

**Better alternative**: One rolling window at 90 days, keep `call_date` column, entity vars filter within:
```yaml
- entity_var:
    name: sf_call_count_last_30
    select: sum(daily_call_count)
    from: models/gong_calls_90d
    where: call_date > {{end_time_sql}} - INTERVAL '30 day'
```

This is simpler (one rolling model instead of three) and still gets the pre-aggregation benefit.

#### `app_tracks_90d`

```yaml
- name: app_tracks_90d
  model_type: sql_template
  model_spec:
    single_sql: |
      {% set rangeStart = this.GetWhtContext().TimeInfo.Subtract("2160h") %}
      {% set rangeEnd = this.GetWhtContext().TimeInfo %}
      {% set pastMats = this.MultiDeRef('models/app_tracks_daily_agg',
          pre_existing=true, range_start=rangeStart, range_end=rangeEnd) %}
      SELECT CONTEXT_TRAITS_WORKSPACE_ID, EVENT_TEXT, event_date,
             SUM(daily_count) as daily_count
      FROM (
          {% for mat in pastMats %}
              {% if not loop.first %}UNION ALL{% endif %}
              SELECT * FROM {{mat}}
          {% endfor %}
      )
      GROUP BY 1, 2, 3
    ids:
      - select: CONTEXT_TRAITS_WORKSPACE_ID
        type: workspace_id
        entity: workspace
    materialization:
      output_type: ephemeral
```

Entity vars:
```yaml
- entity_var:
    name: app_number_destinations_deleted_days_last_7
    select: sum(daily_count)
    from: models/app_tracks_90d
    where: EVENT_TEXT = 'destination deleted' and event_date > {{end_time_sql}} - INTERVAL '7 day'
```

### Layer 2 for Event Volume

For `daily_event_volume` — the source is already daily, so the rolling window just unions daily materializations:

```yaml
- name: daily_event_volume_90d
  model_type: sql_template
  model_spec:
    single_sql: |
      {% set rangeStart = this.GetWhtContext().TimeInfo.Subtract("2160h") %}
      {% set rangeEnd = this.GetWhtContext().TimeInfo %}
      {% set pastMats = this.MultiDeRef('models/daily_event_volume_daily_agg',
          pre_existing=true, range_start=rangeStart, range_end=rangeEnd) %}
      SELECT WORKSPACEID, REPORT_DATE, DAILY_TOTAL
      FROM (
          {% for mat in pastMats %}
              {% if not loop.first %}UNION ALL{% endif %}
              SELECT WORKSPACEID, REPORT_DATE, DAILY_TOTAL FROM {{mat}}
          {% endfor %}
      )
    ids:
      - select: WORKSPACEID
        type: workspace_id
        entity: workspace
    materialization:
      output_type: ephemeral
```

For `monthly_event_volume` — similar pattern with 13-month range.

## Entity Var Changes

| Current `from:` | New `from:` | Vars | Aggregation Change |
|-----------------|-------------|------|-------------------|
| `inputs/gong_call` | `models/gong_calls_90d` | 12 | `count(*)` → `sum(daily_call_count)`, `sum(DURATION)` → `sum(daily_duration)` |
| `models/recent_app_tracks` | `models/app_tracks_90d` | 6 | `count(*)` → `sum(daily_count)` |
| `inputs/daily_event_volume` | `models/daily_event_volume_90d` | 3 | No change (passthrough daily) |
| `inputs/monthly_event_volume` | `models/monthly_event_volume_13m` | 13 | No change (passthrough monthly) |
| `models/recent_app_identifies` | **Keep as Plan A** | 9 | Cannot pre-aggregate COUNT(DISTINCT) |

## What This Doesn't Cover

| Category | Why | Mitigation |
|----------|-----|-----------|
| `count(DISTINCT USER_ID)` vars (identifies) | Tumbling windows lose dedup across days | Keep Plan A bounded-window model |
| Thena ticket vars (14) | Mutable dedup view, low volume | Full rescan is cheap; future: CDC model |
| `any_value()` dimension lookups (~50) | Not aggregatable — need current row value | Needs CDC approach |
| Unbounded counts from mutable sources (~30) | Rows change, no time boundary | Needs CDC approach |

## Scorecard

| Metric | Value |
|--------|-------|
| PB engine changes | NONE (if `time_grain` + `MultiDeRef` already work) |
| New models | 4 daily aggs + 4 rolling windows = 8 (+ keep `recent_app_identifies`) |
| Vars with true tumbling-window incrementality | 34 (gong 12 + tracks 6 + daily_vol 3 + monthly_vol 13) |
| Vars with Plan A bounded-window | 9 (identifies DISTINCT) |
| Total incremental / bounded | 43 direct + ~246 rollup/derived = ~289 |
| Pre-aggregation benefit | YES — entity vars scan small daily summaries, not raw events |
| Time to implement | Days to a week (need to validate `MultiDeRef`/`time_grain` APIs first) |
| Risk | Medium — depends on `time_grain`/`MultiDeRef` being production-ready |

## Prerequisites / Validation Steps

1. **Confirm `time_grain: day` works**: Run a minimal project with a daily agg model, verify PB creates per-day materializations
2. **Confirm `MultiDeRef` works**: Verify it returns the right set of materializations for a date range
3. **Confirm `GetWhtContext().TimeInfo.Subtract()` works**: Verify time arithmetic in Pongo2 templates
4. **Test id_cluster_delta join pattern**: Verify entity ID re-keying works with the `COALESCE(d.new_main_id, ...)` pattern
5. **Test on cs-profiles**: Full refresh → incremental run → compare outputs

## Migration Path

1. Start with `gong_calls_daily_agg` + `gong_calls_90d` (simplest, no DISTINCT issues)
2. Add `app_tracks_daily_agg` + `app_tracks_90d` (replaces `recent_app_tracks`)
3. Add event volume models
4. Keep `recent_app_identifies` as-is
5. Remove `recent_app_tracks` once `app_tracks_90d` is validated
