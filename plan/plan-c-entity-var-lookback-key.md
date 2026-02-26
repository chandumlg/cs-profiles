# Plan C: Native `lookback` Key — Tumbling Window Entity Vars in PB SDK

## Core Concept

Add a `lookback` YAML key to entity vars. When present, PB auto-generates **child entity vars** at configurable time grains (daily, hourly, weekly, monthly). The parent entity var's SQL becomes:

1. **Partition** the lookback window into time-grain slices
2. **MultiDeRef** past materializations of the child entity var
3. **Remap IDs** via `id_cluster_delta`
4. **Union** all slices
5. **Merge** (re-aggregate) into the final value

This is tumbling windows built INTO the entity var system — not as separate hand-written SQL models.

---

## YAML Syntax

```yaml
- entity_var:
    name: sf_call_count_last_90
    select: count(*)
    from: inputs/gong_call
    lookback: 90 day               # NEW — activates tumbling window
    # where: still supported, AND-ed with auto time filter
```

**What PB auto-generates internally (conceptual path structure):**

```
entity_var_tables/account/
├── sf_call_count_last_90              # Parent: unions children + remaps IDs + merges
│   ├── sf_call_count_last_90/daily    # Child: count(*) for ONE day's gong_call data
│   ├── sf_call_count_last_90/hourly   # Child: count(*) for ONE hour's data (optional)
│   ├── sf_call_count_last_90/weekly   # Child: count(*) for ONE week's data (optional)
│   └── sf_call_count_last_90/monthly  # Child: count(*) for ONE month's data (optional)
```

The user only writes the parent declaration. PB creates the children automatically.

### Time Grain Selection

```yaml
lookback: 90 day                      # Default grain: day → 90 daily children
lookback: 90 day grain day            # Explicit: daily
lookback: 13 month grain month        # 13 monthly children
lookback: 24 hour grain hour          # 24 hourly children
```

If `grain` is omitted, PB infers the grain from the lookback unit (e.g., `90 day` → grain `day`).

### Multi-Grain Optimization (Future)

```yaml
lookback: 90 day grain [month, day]   # 2 monthly + remaining daily children
```

PB would partition 90 days into: 2 complete months (coarser, fewer materializations) + remaining days. For now, single-grain is sufficient.

---

## Architecture: Child Entity Vars

### What Is a Child Entity Var?

A child entity var is an **internal entity var item** with:
- Same `select:` and `from:` and `where:` as the parent
- `time_grain: day` (or hour/week/month)
- Materialized as a column in a var table with `time_grain: day`
- One materialization per time-grain period (one table per day)
- `is_append_only: false` (the value for a given day is computed once and never changes)

### Child Var Path Structure

For entity var `sf_call_count_last_90` in var group `account_feature_vars`:

```
Path: entity_var_tables/account/account_feature_vars__var_table/sf_call_count_last_90/daily
```

This child is a real entity var item in the model graph, registered via `BSN_AddChildInfo` just like `incr_delta_tbl` children are registered for append-only models.

### Child Var SQL

The child var computes the SAME aggregation as the parent, but PB injects a time-grain boundary filter:

```sql
-- Child: sf_call_count_last_90/daily (for day D)
SELECT main_id, count(*) AS sf_call_count_last_90_daily
FROM var_table
INNER JOIN gong_call USING (main_id)
WHERE main_id IS NOT NULL
  AND DATE >= <day_D_start> AND DATE < <day_D_end>    -- Auto-injected grain boundary
  AND <user_where_clause>                               -- Original where: if any
GROUP BY main_id
```

The time boundary comes from the child's `WhtContext.TimeInfo.BeginTime` / `EndTime`, which are set to the grain boundaries by PB's time grain system (already implemented in `RoundToTimeGrain`).

### What the Child Materializes

Each daily materialization is a small table:

| main_id | sf_call_count_last_90_daily |
|---------|---------------------------|
| acct_001 | 3 |
| acct_002 | 1 |
| acct_005 | 7 |

Only entities with non-zero activity that day appear. This is very compact.

---

## Architecture: Parent Entity Var

### Parent Var SQL

The parent entity var does NOT read from `inputs/gong_call` directly. Instead:

```pongo2
{# Auto-generated SQL for parent entity var with lookback: 90 day #}

{%- set rangeStart = this.GetWhtContext().TimeInfo.Subtract("2160h") -%}
{%- set rangeEnd = this.GetWhtContext().TimeInfo -%}
{%- set pastMats = this.MultiDeRef('self/daily',
        pre_existing=true, range_start=rangeStart, range_end=rangeEnd) -%}
{%- set idDelta = this.DeRef(id_stitcher_path + '/id_cluster_delta',
        dependency="optional", pre_existing=true) -%}

-- Step 1: Union all daily materializations
WITH daily_union AS (
    {% for mat in pastMats %}
        {% if not loop.first %}UNION ALL{% endif %}
        SELECT main_id, sf_call_count_last_90_daily FROM {{ mat }}
    {% endfor %}
),
-- Step 2: Remap IDs via id_cluster_delta
remapped AS (
    SELECT
        {% if idDelta.IsEnabled() %}
            COALESCE(d.new_main_id, u.main_id)
        {% else %}
            u.main_id
        {% endif %} AS main_id,
        sf_call_count_last_90_daily
    FROM daily_union u
    {% if idDelta.IsEnabled() %}
    LEFT JOIN {{ idDelta }} d ON u.main_id = d.old_main_id
    {% endif %}
)
-- Step 3: Merge (re-aggregate)
SELECT main_id, SUM(sf_call_count_last_90_daily) AS sf_call_count_last_90
FROM remapped
GROUP BY main_id
```

### The Five Steps in Detail

| Step | What | How |
|------|------|-----|
| 1. Partition | Determine which daily materializations to include | `MultiDeRef` with `range_start` = end_time - lookback, `range_end` = end_time |
| 2. Retrieve | Get past daily materializations | `MultiDeRef` returns list of `IWhtRecipeFriendlyMaterial` |
| 3. Union | Combine all daily slices | `UNION ALL` in SQL |
| 4. Remap IDs | Handle entity ID changes since old materializations | `LEFT JOIN id_cluster_delta` with `COALESCE(new_main_id, old_main_id)` |
| 5. Merge | Re-aggregate daily values into final result | Inferred merge function (see below) |

---

## Merge Function Inference

The parent must know HOW to re-aggregate daily values. PB infers this from the `select:` expression:

| `select:` pattern | Child computes | Parent merges with | Notes |
|-------------------|---------------|-------------------|-------|
| `count(*)` | `count(*) as daily_val` | `SUM(daily_val)` | count of counts = sum |
| `count(X)` | `count(X) as daily_val` | `SUM(daily_val)` | same |
| `sum(X)` | `sum(X) as daily_val` | `SUM(daily_val)` | sum of sums |
| `max(X)` | `max(X) as daily_val` | `MAX(daily_val)` | max of maxes |
| `min(X)` | `min(X) as daily_val` | `MIN(daily_val)` | min of mins |
| `avg(X)` | `sum(X) as daily_sum, count(X) as daily_cnt` | `SUM(daily_sum) / NULLIF(SUM(daily_cnt), 0)` | Need two child columns |
| `count(DISTINCT X)` | **Cannot pre-aggregate** | — | Falls back to bounded-window approach |
| `any_value(X)` | **Cannot meaningfully aggregate** | — | Falls back / error |
| Complex expression | Parse and decompose, or fallback | — | Phase 2+ |

### Fallback Behavior

When PB cannot infer a merge function (e.g., `count(DISTINCT)`), two options:

**Option A — Bounded window fallback**: PB creates a single child that materializes ALL raw rows within the lookback window (like `recent_app_tracks`). No pre-aggregation, but still benefits from incremental delta.

**Option B — Error**: PB rejects `lookback` on non-mergeable aggregations. User must handle manually via intermediate SQL models.

**Recommendation**: Option A for Phase 1 (pragmatic), Option B for Phase 2 (forces clean patterns).

### Explicit Merge Override

For complex cases, allow the user to specify the merge function:

```yaml
- entity_var:
    name: avg_call_duration_last_90
    select: avg(DURATION)
    from: inputs/gong_call
    lookback: 90 day
    lookback_merge: SUM(daily_sum) / NULLIF(SUM(daily_cnt), 0)
    lookback_child_select: SUM(DURATION) as daily_sum, COUNT(DURATION) as daily_cnt
```

This is for Phase 2. Phase 1 handles only `count`, `sum`, `max`, `min`.

---

## Relationship to Existing `delta_update`

PB already has a `delta_update` mechanism on entity vars. Key differences:

| Aspect | `delta_update` (existing) | `lookback` (proposed) |
|--------|--------------------------|----------------------|
| Purpose | Serve delta updates to feature views | Compute the entity var itself incrementally |
| Where it acts | Feature view output layer | Entity var computation layer |
| Creates | Delta cohort (filters to new entities) | Child entity vars (pre-aggregated slices) |
| Uses MultiDeRef | Yes, in feature view SQL | Yes, in parent entity var SQL |
| Time grain | On the cohort | On the child entity var |

They are complementary. A var can have BOTH `lookback` (for incremental computation) and `delta_update` (for incremental serving).

### Reuse of Existing Infrastructure

The `lookback` feature can reuse:
- `TimeGrainType` and `RoundToTimeGrain()` — already in `core/base/timeGrains.go`
- `MultiDeRef` — already in `core/base/recipeFriendlyMaterial.go`
- `id_cluster_delta` — already in `core/identity/idStitcher/idStitcherDelta.sql`
- `BSN_AddChildInfo` — already in `core/base/buildSpecNotifications.go` for registering children
- `GetPastMaterialsInTimeRange` — already in context.go
- `ParseTimeGrainSpec` — already in `core/base/timeGrains.go`

---

## Implementation Plan

### Phase 1: Parsing + Child Generation (No SQL change yet)

**Goal**: PB recognizes `lookback:`, creates child entity var items, validates.

#### File: `core/base/buildspec.go`

Add `Lookback` and `LookbackGrain` to `BaseVarDeclaration`:

```go
type BaseVarDeclaration struct {
    BaseModelBuildSpec `yaml:",inline"`

    Name           string            `yaml:"name"`
    Select         string            `yaml:"select"`
    From           PathRefBuildSpec  `yaml:"from,omitempty"`
    Where          string            `yaml:"where,omitempty"`
    Lookback       string            `yaml:"lookback,omitempty"`       // NEW: "90 day"
    LookbackGrain  TimeGrainType     `yaml:"lookback_grain,omitempty"` // NEW: "day" (optional)
    // ... existing fields ...
}
```

Add `LookbackSpec` parsed type:

```go
type LookbackSpec struct {
    Duration  int           // 90
    Unit      TimeGrainType // day
    Grain     TimeGrainType // day (defaults to Unit)
}
```

#### File: `core/base/vars.go`

Parse `lookback` in `GetBaseVarFromBuildSpec()`:

```go
func GetBaseVarFromBuildSpec(bVar BaseVarDeclaration) *WhtModelBaseVar {
    ev := &WhtModelBaseVar{
        Name:       bVar.Name,
        SelectSql:  bVar.Select,
        WhereSql:   bVar.Where,
        From:       bVar.From,
        // ... existing ...
    }
    if bVar.Lookback != "" {
        spec, err := ParseLookbackSpec(bVar.Lookback, bVar.LookbackGrain)
        if err != nil { /* ... */ }
        ev.LookbackSpec = spec
    }
    return ev
}
```

#### File: `core/base/types.go`

Add `LookbackSpec` to `WhtModelBaseVar`:

```go
type WhtModelBaseVar struct {
    // ... existing fields ...
    LookbackSpec *LookbackSpec  // NEW: parsed lookback declaration
}
```

Add to `HashComponents()`:

```go
func (v *WhtModelBaseVar) HashComponents() map[string]string {
    h := map[string]string{
        // ... existing ...
    }
    if v.LookbackSpec != nil {
        h["Lookback"] = utils.HashString(fmt.Sprintf("%d_%s_%s",
            v.LookbackSpec.Duration, v.LookbackSpec.Unit, v.LookbackSpec.Grain))
    }
    return h
}
```

#### File: `core/entityVarItem/model.go`

When creating the entity var item, if `lookback` is set, register child entity vars via `BSN_AddChildInfo`:

```go
func createEntityVarItemModel(parentFolder base.IWhtFolder, modelName string,
    buildSpec base.IWhtBuildSpec, hooks map[base.RecipeHookTypeEnum]string,
) (model base.IReferable, finishPending func() error, err error) {
    // ... existing creation logic ...

    if entityVar.LookbackSpec != nil {
        // Register child entity var for the grain
        childName := string(entityVar.LookbackSpec.Grain)  // e.g., "daily"
        childBuildSpec := deriveChildBuildSpec(eviBuildSpec, entityVar.LookbackSpec)

        messages := []*base.SpecUpdateMessage{
            {
                UpdateType: base.BSN_AddChildInfo,
                Payload: &base.AddChildrenPayload{
                    BuildInfos: []*base.AddChildPayLoad{
                        {
                            Name:      childName,
                            Type:      utils.EntityVarItemType,
                            BuildSpec: childBuildSpec,
                        },
                    },
                },
            },
        }
        // Send messages to register child
        for _, msg := range messages {
            if err := parentFolder.HandleSpecUpdate(msg); err != nil {
                return nil, nil, err
            }
        }
    }
    // ...
}
```

`deriveChildBuildSpec()` creates a child entity var build spec:
- Same `select`, `from`, `where` as parent
- `time_grain` = lookback grain (e.g., `day`)
- `materialization.output_type` = `column` (in a time-grained var table)
- No `lookback` on the child (prevents recursion)

#### File: `core/entityVarItem/types.go`

Add `LookbackSpec` field:

```go
type EntityVarItemModel struct {
    *base.VarItemModel
    IsFeature    bool
    Merge        string
    MergeWhere   string
    DeltaUpdate  base.TimeGrainType
    LookbackSpec *base.LookbackSpec  // NEW
    eviBuildSpec *EntityVarItemModelBuildSpec
}
```

### Phase 2: Parent SQL Generation

**Goal**: Parent entity var generates the union+remap+merge SQL instead of reading from `from:` directly.

#### File: `core/entityVarItem/modelCreatorSqlHelper.sql`

Add a new code path for lookback vars:

```pongo2
{%- if entityVar.LookbackSpec -%}
    {# Tumbling window path #}
    {%- set lookbackHours = entityVar.LookbackSpec.DurationInHours -%}
    {%- set rangeStart = this.GetWhtContext().TimeInfo.Subtract(lookbackHours) -%}
    {%- set rangeEnd = this.GetWhtContext().TimeInfo -%}
    {%- set childPath = this.Model.GetPath() + "/" + entityVar.LookbackSpec.Grain -%}
    {%- set pastMats = this.MultiDeRef(childPath,
            pre_existing=true, range_start=rangeStart, range_end=rangeEnd) -%}
    {%- set idDelta = this.DeRef(id_stitcher_delta_path,
            dependency="optional", pre_existing=true) -%}

    WITH daily_union AS (
        {% for mat in pastMats %}
            {% if not loop.first %}UNION ALL{% endif %}
            SELECT {{ main_id }}, {{ entityVar.ChildColumnName }} FROM {{ mat }}
        {% endfor %}
    ),
    remapped AS (
        SELECT
            {% if idDelta.IsEnabled() %}
                COALESCE(d.new_main_id, u.{{ main_id }})
            {% else %}
                u.{{ main_id }}
            {% endif %} AS {{ main_id }},
            {{ entityVar.ChildColumnName }}
        FROM daily_union u
        {% if idDelta.IsEnabled() %}
        LEFT JOIN {{ idDelta }} d ON u.{{ main_id }} = d.old_main_id
        {% endif %}
    )
    SELECT {{ main_id }},
           {{ entityVar.MergeFunction }}({{ entityVar.ChildColumnName }})
             AS {{ ev_col_name }}
    FROM remapped
    GROUP BY {{ main_id }}

{%- else -%}
    {# Existing non-lookback path — unchanged #}
    ...
{%- endif -%}
```

### Phase 3: Merge Inference

**Goal**: PB parses `select:` to infer the correct merge function.

#### File: `core/base/lookback.go` (new)

```go
package base

import "fmt"

type MergeStrategy struct {
    ChildSelect string   // What the child entity var computes
    MergeExpr   string   // How parent re-aggregates
    ChildCols   []string // Column names from child
}

// InferMergeStrategy parses the select expression and returns the merge strategy.
func InferMergeStrategy(selectExpr string) (*MergeStrategy, error) {
    // Parse outer aggregation function
    fn, inner := parseOuterAggFunction(selectExpr)

    switch fn {
    case "count":
        if isDistinct(inner) {
            return nil, fmt.Errorf("count(DISTINCT) cannot be pre-aggregated; "+
                "lookback will use bounded-window fallback")
        }
        return &MergeStrategy{
            ChildSelect: selectExpr,
            MergeExpr:   "SUM",
            ChildCols:   []string{"daily_val"},
        }, nil
    case "sum":
        return &MergeStrategy{
            ChildSelect: selectExpr,
            MergeExpr:   "SUM",
            ChildCols:   []string{"daily_val"},
        }, nil
    case "max":
        return &MergeStrategy{
            ChildSelect: selectExpr,
            MergeExpr:   "MAX",
            ChildCols:   []string{"daily_val"},
        }, nil
    case "min":
        return &MergeStrategy{
            ChildSelect: selectExpr,
            MergeExpr:   "MIN",
            ChildCols:   []string{"daily_val"},
        }, nil
    case "avg":
        return &MergeStrategy{
            ChildSelect: fmt.Sprintf("SUM(%s) as daily_sum, COUNT(%s) as daily_cnt", inner, inner),
            MergeExpr:   "SUM(daily_sum) / NULLIF(SUM(daily_cnt), 0)",
            ChildCols:   []string{"daily_sum", "daily_cnt"},
        }, nil
    default:
        return nil, fmt.Errorf("cannot infer merge for aggregation '%s'; "+
            "specify lookback_merge explicitly", fn)
    }
}
```

### Phase 4: Bounded-Window Fallback for COUNT(DISTINCT)

When merge inference fails (e.g., `count(DISTINCT USER_ID)`), PB falls back to creating a single child that materializes ALL raw rows within the lookback window — identical to the `recent_app_tracks` pattern but auto-generated.

The child's SQL:
```sql
-- Bounded-window child (no pre-aggregation)
-- Like recent_app_tracks: UNION ALL(lastThis, delta), filter to window

{%- set lastThis = this.DeRef(pre_existing=true, dependency="optional", checkpoint_name="baseline") -%}
{% set inputDelta = this.DeRef(from_path + "/incr_delta_tbl", dependency="coercive",
    baseline_name="baseline", prereqs=[lastThis]) %}
{% set inputFull = this.DeRef(from_path, prereqs=[inputDelta.Except()]) %}

{% if lastThis.IsEnabled() and inputDelta.IsEnabled() %}
  SELECT * FROM (
    SELECT <columns> FROM {{ lastThis }}
    UNION ALL
    SELECT <columns> FROM {{ inputDelta }}
  )
  WHERE <occurred_at_col> > {{ end_time_sql }} - INTERVAL '<lookback>'
{% else %}
  SELECT * FROM (
    SELECT <columns> FROM {{ inputFull }}
    WHERE <occurred_at_col> > {{ end_time_sql }} - INTERVAL '<lookback>'
  )
{% endif %}
```

The parent entity var then reads from this child like a normal `from:` source.

---

## Var Grouping Optimization

Multiple entity vars with `lookback` from the same input can share children.

```yaml
# These three vars all read from inputs/gong_call:
sf_call_count_last_90:  lookback: 90 day
sf_call_count_last_60:  lookback: 60 day
sf_call_count_last_30:  lookback: 30 day
```

PB could:
1. **Separate children per var** (simplest) — each var gets its own daily child. Simple but creates many models.
2. **Shared daily child per (input, where_clause) group** — one daily child for all gong_call vars. Each parent MultiDeRefs with its own range. The daily child computes both `count(*)` and `sum(DURATION)`.

**Recommendation**: Phase 1 uses separate children. Phase 2 adds grouping optimization.

For shared children, the daily child would need to compute multiple aggregations:

```sql
-- Shared daily child for gong_call vars
SELECT main_id,
       count(*) as daily_call_count,
       sum(DURATION) as daily_duration
FROM gong_call
WHERE main_id IS NOT NULL AND DATE >= <day_start> AND DATE < <day_end>
GROUP BY main_id
```

And each parent picks the column it needs:
```sql
-- sf_call_count_last_90 parent
SELECT main_id, SUM(daily_call_count) AS sf_call_count_last_90
FROM remapped GROUP BY main_id

-- sf_call_duration_last_90 parent
SELECT main_id, SUM(daily_duration) AS sf_call_duration_last_90
FROM remapped GROUP BY main_id
```

---

## cs-profiles: What Gets `lookback`

### Entity Vars That Become Tumbling-Window (count/sum — mergeable)

| Input | Vars | Lookback | Grain | Merge |
|-------|------|----------|-------|-------|
| `inputs/gong_call` | `sf_call_count_last_30/60/90` (x2 cohorts = 6) | 30/60/90 day | day | SUM(daily_count) |
| `inputs/gong_call` | `sf_call_duration_last_30/60/90` (x2 cohorts = 6) | 30/60/90 day | day | SUM(daily_sum) |
| `inputs/thena_tickets` | `thena_all_tickets_created_last_10/30/60/90` (x2 = 8) | 10/30/60/90 day | day | SUM(daily_count) |
| `inputs/thena_tickets` | `thena_all_escalated_tickets_created_last_15` (x2 = 2) | 15 day | day | SUM(daily_count) |
| `inputs/app_tracks` | `destinations_deleted_last_7/30/90` (3) | 7/30/90 day | day | SUM(daily_count) |
| `inputs/app_tracks` | `sources_deleted_last_7/30/90` (3) | 7/30/90 day | day | SUM(daily_count) |
| `inputs/daily_event_volume` | `event_volume_days_last_30/60/90` (3) | 30/60/90 day | day | SUM(daily_total) |
| `inputs/monthly_event_volume` | `event_volume_month_to_date` through `month_12` (13) | 13 month | month | passthrough (each month is point query) |
| `inputs/app_transformations` | `created_last_7/30/90` (3) | 7/30/90 day | day | SUM(daily_count) |
| `inputs/app_destinations` | `created_last_7/30/90` (3) | 7/30/90 day | day | SUM(daily_count) |
| `inputs/app_sources` | `created_last_7/30/90` (3) | 7/30/90 day | day | SUM(daily_count) |
| **Total** | **53** | | | |

### Entity Vars That Use Bounded-Window Fallback (count DISTINCT — not mergeable)

| Input | Vars | Lookback | Reason |
|-------|------|----------|--------|
| `inputs/app_identifies` | `distinct_users_workspaces_last_7/30/90` (x2 entities = 6) | 7/30/90 day | `count(DISTINCT USER_ID)` |
| `inputs/app_identifies` | `distinct_users_organizations_last_7/30/90` (3) | 7/30/90 day | `count(DISTINCT USER_ID)` |
| **Total** | **9** | | |

These 9 vars get the same behavior as current `recent_app_identifies` but auto-generated.

### Entity Vars That Do NOT Get `lookback` (unbounded or non-time-windowed)

| Category | Count | Reason |
|----------|-------|--------|
| `any_value()` dimension lookups | ~50 | Not aggregatable, mutable source |
| Unbounded `count(*)` from mutable sources | ~30 | No time boundary — needs CDC |
| `count(DISTINCT)` from mutable (app_cs_connections) | 22 | Mutable multi-join view |
| Gong pre/post sale vars | ~10 | Boundary = signed_date, not clock time |
| Thena `currently_open`/`currently_closed` | 4 | Unbounded, need current state |
| Rollup/derived vars | ~238 | Inherit from source vars |

### Resulting YAML (example)

**Before:**
```yaml
- entity_var:
    name: sf_call_count_last_90
    select: count(*)
    from: inputs/gong_call
    where: DATE > {{end_time_sql}} - INTERVAL '90 day'
```

**After:**
```yaml
- entity_var:
    name: sf_call_count_last_90
    select: count(*)
    from: inputs/gong_call
    lookback: 90 day
```

No `where:` needed for the time filter — PB handles it. If the var has additional non-time conditions:

```yaml
- entity_var:
    name: thena_all_escalated_tickets_created_last_15_days
    select: count(*)
    from: inputs/thena_tickets
    where: ESCALATION_TYPE = 'manual' and STATUS <> 'NOT_A_REQUEST' and STATUS <> 'MERGED'
    lookback: 15 day
```

The `where:` is preserved on the child (filters rows before aggregation), AND-ed with the auto time filter.

### What Gets Eliminated

With `lookback`, the hand-written intermediate SQL models become unnecessary:
- `recent_app_tracks` — **deleted** (replaced by auto-generated daily child of tracks vars)
- `recent_app_identifies` — **deleted** (replaced by auto-generated bounded-window child of identifies vars)

---

## Monthly Event Volume: Special Case

The monthly event volume vars use point queries, not range aggregations:

```yaml
# Current:
- entity_var:
    name: event_volume_month_3
    select: COALESCE(AVG(MONTHLY_TOTAL), 0)
    from: inputs/monthly_event_volume
    where: REPORT_DATE = DATE_TRUNC('month', {{end_time_sql}}) - INTERVAL '3 month'
```

With `lookback`, this becomes:

```yaml
- entity_var:
    name: event_volume_month_3
    select: COALESCE(AVG(MONTHLY_TOTAL), 0)
    from: inputs/monthly_event_volume
    where: REPORT_DATE = DATE_TRUNC('month', {{end_time_sql}}) - INTERVAL '3 month'
    lookback: 4 month
    lookback_grain: month
```

The `lookback: 4 month` bounds the scan to 4 months of data. The `where:` further narrows to the exact month. The daily/monthly child materializes all monthly_event_volume rows for that grain period. The parent re-queries from the child.

Alternatively, since these are point queries (not aggregations over windows), `lookback` might be overkill. Consider leaving these as-is or using a simpler bounded-scan approach.

---

## PB SDK Files to Modify

| Phase | File | Change |
|-------|------|--------|
| 1 | `core/base/buildspec.go` | Add `Lookback`, `LookbackGrain` to `BaseVarDeclaration` |
| 1 | `core/base/types.go` | Add `LookbackSpec` to `WhtModelBaseVar`, update `HashComponents()` |
| 1 | `core/base/vars.go` | Parse `lookback` in `GetBaseVarFromBuildSpec()` |
| 1 | **`core/base/lookback.go` (new)** | `LookbackSpec`, `ParseLookbackSpec()`, `InferMergeStrategy()` |
| 1 | `core/entityVarItem/model.go` | Register child entity vars via `BSN_AddChildInfo` when `lookback` is set |
| 1 | `core/entityVarItem/types.go` | Add `LookbackSpec` to `EntityVarItemModel` |
| 1 | `core/entityVarItem/buildspec.go` | Pass `LookbackSpec` through build spec unification |
| 2 | `core/entityVarItem/modelCreatorSqlHelper.sql` | Add tumbling-window SQL path (MultiDeRef + remap + merge) |
| 2 | `core/entityVarItem/model.go` | Add `GetChildPath()`, `GetMergeFunction()` methods |
| 3 | `core/base/lookback.go` | `InferMergeStrategy()` — parse select expressions |
| 3 | `core/entityVarItem/model.go` | Validation: reject lookback on non-mergeable aggregations (or fallback) |
| 4 | `core/entityVarItem/modelCreatorSqlHelper.sql` | Bounded-window fallback path for count(DISTINCT) |

---

## Scorecard

| Metric | Value |
|--------|-------|
| PB engine changes | Significant (4 phases) |
| Vars with tumbling-window incrementality | 53 |
| Vars with bounded-window fallback | 9 |
| Total with rollup/derived | ~306 of 413 (74%) |
| Intermediate models eliminated | 2 (recent_app_tracks, recent_app_identifies) |
| Reusability | Universal — any PB project benefits |
| Maintenance per project | Near zero (declarative YAML, no SQL models) |
| Time to implement Phase 1+2 | 1-2 weeks |
| Time to implement Phase 3+4 | 1-2 weeks additional |

---

## Comparison with Plan A and Plan B

| Dimension | Plan A (bounded window) | Plan B (hand-written tumbling SQL) | Plan C (lookback key) |
|-----------|------------------------|-----------------------------------|----------------------|
| Approach | `UNION ALL(lastThis, delta)` then re-filter | Daily agg + MultiDeRef + union (hand-written) | Same as B but auto-generated |
| PB changes | None | None (if MultiDeRef works) | Significant |
| Pre-aggregation | No | Yes | Yes |
| Models to maintain | 5 (hand-written) | 9 (hand-written) | 0 (auto-generated) |
| COUNT(DISTINCT) | Works (raw rows preserved) | Falls back to Plan A | Auto-fallback to bounded window |
| ID remapping | In intermediate model SQL | In rolling window SQL | Auto-generated |
| Reusability | None (project-specific) | None (project-specific) | Universal |
| Time to ship | Hours | Days | Weeks |

---

## Recommended Sequence

1. **Now (hours)**: Ship Plan A for cs-profiles — immediate value, prove correctness
2. **Next sprint**: Build Plan C Phase 1+2 — parsing + child generation + parent SQL
3. **Following sprint**: Build Plan C Phase 3+4 — merge inference + fallback
4. **Then**: Migrate cs-profiles from Plan A to Plan C (delete intermediate models, add `lookback:` to vars)
5. **Future**: Add `is_cdc_capable` contract flag for mutable sources → close remaining ~90 vars

Plan B is skipped. It's the same architecture as Plan C but with manual labor. If we're going to build tumbling windows, build them into the SDK.

---

## Verification Plan

1. **Unit tests**: Parse lookback spec, infer merge strategy, validate child generation
2. **Compile test**: Project with lookback vars compiles to correct SQL
3. **Integration test**: Run on cs-profiles, compare outputs:
   - Full refresh (original) vs full refresh (with lookback) — must match
   - Incremental (with lookback) vs full refresh — must match
4. **Performance test**: Measure wall-clock time for incremental vs full refresh
