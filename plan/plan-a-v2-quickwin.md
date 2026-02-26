# Plan A v2: Quick Win — Mark Base Tables Append-Only

## How to Verify This Plan

All claims can be verified against:
- **Project files**: `made_incremental/models/{inputs.yaml, profiles.yaml, sql_models.yaml}`
- **Warehouse results**: `made_incremental/results` (DDLs, column listings, table types)
- **Original project**: `orig_project/models/` (baseline for comparison)

Key verification commands (run from `made_incremental/models/`):
```bash
grep -c "  - entity_var:" profiles.yaml                # Active vars → 407
grep -c "# - entity_var:" profiles.yaml                # Commented-out vars → 6 (sf_opportunity)
grep "^  - name:" inputs.yaml | wc -l                  # Total inputs → 19
grep -c "is_append_only: true" inputs.yaml             # Currently incremental → 4
```

## Problem

The cs-profiles project computes 407 entity vars across two entities (workspace, account) with two cohorts (AllCustomersProspects, CurrentCustomers). Today, every run does a full scan of every input table.

## Key Insight

There are only two kinds of inputs:

1. **Append-only event streams** — each row is an independent event (`app_tracks`, `app_identifies`)
2. **Append-only CDC streams** — each row is a state change to a mutable entity (`app_sources`, `app_destinations`, Stitch-synced tables, Salesforce-synced tables)

There is a third category that doesn't fit cleanly: **external pipeline outputs** like dbt models (`monthly_event_volume`, `daily_event_volume` — backed by `ANALYTICS.analytics.agg_event_volume_daily_with_sfdc`). These are rebuilt by an external process we don't control. 16 vars depend on this — they remain full-scan.

The remaining **mutable snapshot views** (`gong_call`, `sf_account_view`, `app_user_accounts`, `app_cs_connections`) are convenience layers over base tables that ARE append-only. All 4 verified via DDL inspection (see `made_incremental/results`). These are addressed in the follow-up plan (`plan-a-v2-fewdays-win.md`).

PB handles both event streams and CDC with the same machinery:
- `is_append_only: true` on the input
- Entity vars with `merge:` for incremental computation

| Pattern | select | merge | Entity is... |
|---------|--------|-------|-------------|
| Event stream | `count(*)`, `sum(X)` | `sum(rowset.col)` | External (workspace, account) |
| CDC | `MAX_BY(state, ts)` | `MAX_BY(rowset.state, rowset.ts)` | The record itself (source, ticket) |

## Current State

### Inputs (19 in inputs.yaml)

| Input | Actual Table | `is_append_only` | Nature |
|-------|-------------|-------------------|--------|
| `app_tracks` | `RUDDERWEBAPP.TRACKS` | `true` | Event stream |
| `app_identifies` | `RUDDERWEBAPP.IDENTIFIES` | `true` | Event stream |
| `thena_ticket_create` | `THENA._CREATE` | `true` | CDC (append-only base) |
| `thena_ticket_update` | `THENA._UPDATE` | `true` | CDC (append-only base) |
| `app_sources` | `PPW_PUBLIC.SOURCES` | not set | CDC (Stitch sync) |
| `app_destinations` | `PPW_PUBLIC.DESTINATIONS` | not set | CDC (Stitch sync) |
| `app_transformations` | `PPW_PUBLIC.TRANSFORMATIONS` | not set | CDC (Stitch sync) |
| `app_workspaces` | `PPW_PUBLIC.WORKSPACES` | not set | CDC (Stitch sync) |
| `app_user_roles` | `PPW_PUBLIC.ORGANIZATION_USER_ROLES` | not set | CDC (Stitch sync) |
| `app_tracking_plans` | `PPW_PUBLIC.DG_SOURCE_TRACKINGPLAN_CONFIGS` | not set | CDC (Stitch sync) |
| `app_profiles` | `PPW_PUBLIC.WHT_PROJECTS` | not set | CDC (Stitch sync) |
| `sf_account` | `SF_ACCOUNT` | not set | CDC (Salesforce sync) |
| `sf_account_view` | VIEW: `SALESFORCE_VIEW_TABLE` | not set | Mutable view |
| `gong_call` | VIEW: `GONG_CALLS` | not set | Mutable view |
| `app_user_accounts` | VIEW: `ACCOUNT_VIEW_TABLE` | not set | Mutable view |
| `app_cs_connections` | VIEW: `CUSTOMER_SUCCESS_CONNECTIONS` | not set | Mutable view |
| `monthly_event_volume` | VIEW → dbt model | not set | External pipeline |
| `daily_event_volume` | VIEW → dbt model | not set | External pipeline |
| `thena_tickets` | VIEW (legacy) | not set | Replaced by _create/_update |

### Entity Vars by Source (407 active)

Verified by: `grep -c "  - entity_var:" profiles.yaml` → 407.
Counts verified by: `grep -c "from: <source>" profiles.yaml`.
Note: `inputs/app_workspaces` and `inputs/sf_account` grep counts include 1 id_stitcher `edge_source:` each — subtract 1 for entity var count.

| Source | Vars | Pattern |
|--------|------|---------|
| **Workspace vars** | | |
| `inputs/app_workspaces` | 15 | `any_value(col)` lookups + `count(id)` |
| `inputs/app_destinations` | 5 | `count(ID) WHERE DELETED='FALSE'` |
| `inputs/app_sources` | 6 | `count(ID) WHERE DELETED='FALSE'` + time-windowed |
| `inputs/app_transformations` | 6 | `count(ID)` + time-windowed |
| `inputs/app_tracking_plans` | 1 | `count(DISTINCT)` |
| `inputs/app_profiles` | 1 | `count(DISTINCT)` |
| `inputs/app_cs_connections` | 22 | `count(DISTINCT col)` |
| `inputs/monthly_event_volume` | 13 | `AVG(MONTHLY_TOTAL) WHERE date = month_N` |
| `inputs/daily_event_volume` | 3 | `SUM(DAILY_TOTAL)` |
| `models/recent_app_tracks` | 6 | `count(*) WHERE event = X AND ts > last_N` |
| `models/recent_app_identifies` | 9 | `count(DISTINCT USER_ID)` (3 workspace + 6 account) |
| **Account vars** | | |
| `inputs/sf_account` | 35 | `any_value(col)` lookups (basic + 2 cohorts) |
| `inputs/app_user_accounts` | 8 | `any_value(col)` lookups |
| `inputs/sf_account_view` | 4 | `any_value(TAM/CSM_NAME)` |
| `inputs/gong_call` | 16 | `count(*)`, `sum(DURATION)` time-windowed |
| `inputs/app_user_roles` | 2 | `count(USERID)` |
| `models/thena_current_state` | 14 | `count(*) WHERE status/date filter` |
| **Rollups** | | |
| `models/rolledUpWorkspaces` | 238 | `sum(workspace_var)` — inherits incrementality |
| **Derived (no `from:`)** | 3 | `app_event_storage`, `event_volume_last_90_day_average_contract_utilization`, `product_health_score` |
| **Total** | **407** | |

### Already Implemented (29 vars incremental)

| Model | Source | Vars Served |
|-------|--------|-------------|
| `recent_app_tracks` | `inputs/app_tracks` (append-only) | 6 workspace vars |
| `recent_app_identifies` | `inputs/app_identifies` (append-only) | 9 (3 workspace + 6 account) |
| `thena_current_state` | `thena_ticket_create` + `thena_ticket_update` (append-only) | 14 account vars |

### Data Flow

```
inputs (base tables) ──→ workspace entity vars ──→ workspace_profile (VIEW)
                                                          │
                                                    rolledUpWorkspaces (SELECT * re-keyed to account)
                                                          │
                                                          ▼
inputs (base tables) ──→ account entity vars ◄────── rollup vars from rolledUpWorkspaces
                                │
                                ▼
                          account_profile (VIEW)
                                │
                         ┌──────┴──────┐
                   AllCustomers    CurrentCustomers
                   Prospects       (cohort)
                   (cohort)
```

`workspace_profile` and `account_profile` are late-binding VIEWs over var tables — zero-cost.

---

## The Quick Win: Mark 8 Base Tables Append-Only

Every Stitch-synced and Salesforce-synced base table is already append-only at the storage layer. Stitch appends new rows with `_SDC_BATCHED_AT`. Salesforce sync appends with `SYSTEMMODSTAMP`. All verified in `made_incremental/results`.

### Change: Add `is_append_only: true` in `inputs.yaml`

| Input | CDC Timestamp Column | Verified In |
|-------|---------------------|-------------|
| `app_sources` | `UPDATEDAT` / `_SDC_BATCHED_AT` | `results` line 1167+ |
| `app_destinations` | `UPDATEDAT` / `_SDC_BATCHED_AT` | `results` line 1084+ |
| `app_transformations` | `UPDATEDAT` / `_SDC_BATCHED_AT` | `results` line 1193+ |
| `app_workspaces` | `_SDC_BATCHED_AT` | `results` line 1250+ |
| `app_user_roles` | `UPDATEDAT` / `_SDC_BATCHED_AT` | `results` line 1139+ |
| `app_tracking_plans` | `UPDATEDAT` / `_SDC_BATCHED_AT` | `results` line 1109+ |
| `app_profiles` | `_SDC_BATCHED_AT` | `results` line 1247+ |
| `sf_account` | `SYSTEMMODSTAMP` | `results` line 898+ |

### Effect

PB auto-generates `incr_delta_tbl` for each, enabling entity vars with `merge:` to process only new rows on incremental runs.

### Vars That Immediately Benefit

`any_value(col)` and `max(col)` are aggregation-composable: `any_value(any_value(x), any_value(y)) = any_value(x, y)`. These can safely use `merge:` on CDC inputs:

- From `inputs/app_workspaces` (8 vars): `app_workspace_id`, `app_organization_id`, `app_workspace_environment`, `app_workspace_name`, `app_event_audit_enabled`, `app_rudder_storage`, `app_self_storage`, `app_data_residency`
- From `inputs/sf_account` — basic cohort (5 vars): `sf_account_name_basic`, `sf_account_id_basic`, `sf_account_status_basic`, `sf_account_sub_status_basic`, `sf_plan_type_basic`
- From `inputs/sf_account` — AllCustomersProspects (15 vars): `sf_account_name`, `sf_account_id`, `sf_account_status`, `sf_account_sub_status`, `sf_plan_type`, `sf_domain`, `sf_contracted_event_volume`, `sf_arr`, `sf_kickoff_date`, `sf_customer_signed_date`, `sf_go_live_date`, `sf_customer_health_color`, `sf_customer_health_score` (max), `sf_slack_channel_name`, `sf_internal_slack_channel_name`
- From `inputs/sf_account` — CurrentCustomers (15 vars): same as above

**43 vars become incremental** with `merge: any_value({{rowset.VAR}})` or `merge: max({{rowset.VAR}})`.

### Vars That DON'T Benefit (need CDC entity pattern)

`count(*)` / `count(ID)` / `count(DISTINCT)` are **NOT safe** on CDC tables with simple `merge: sum()`. The same entity can appear in multiple batches (created in batch 1, updated in batch 2), causing double-counting. This applies to ALL count/sum vars from CDC inputs:

- From `app_sources` (6 vars): count vars with DELETED filters + shopify count(DISTINCT)
- From `app_destinations` (5 vars): count vars with DELETED/ENABLED filters
- From `app_transformations` (6 vars): count vars (same ID in multiple batches)
- From `app_tracking_plans` (1 var): count(DISTINCT) with DELETED filter
- From `app_profiles` (1 var): count(DISTINCT) with DELETED filter
- From `app_user_roles` (2 vars): count (same user in multiple batches)
- From `app_workspaces` (7 vars): count vars (workspace total/prod/dev ×2 cohorts, sso_enabled)

Additionally, `count(DISTINCT)` requires HyperLogLog (HLL sketch accumulate + combine) for correct incremental merge. Simple sum of distinct counts overcounts when the same value appears in multiple batches.

These 28 vars need **active section tables** — incremental SQL models that deduplicate each CDC input to its current state (latest row per record, by primary key). Entity vars then read from the deduplicated table instead of the raw input. See `plan-a-v2-fewdays-win.md` Phase 1.

The `thena_current_state` model already implements this pattern for thena tickets. Phase 1 generalizes it to all 8 CDC inputs.

---

## Scorecard

| Metric | Before | After Quick Win |
|--------|--------|----------------|
| Inputs with `is_append_only` | 4 | 12 |
| Vars with incremental computation | 29 | ~72 (29 existing + 43 new) |
| Files changed | — | `inputs.yaml` + `profiles.yaml` (merge clauses) |
| PB SDK changes | None | None |
| Effort | — | Hours |

## Verification

1. Full refresh original project (`orig_project/`) → baseline output
2. Full refresh incremental project (`made_incremental/`) → compare all feature view columns (`FULL OUTER JOIN`, expect zero diffs)
3. Incremental run → compare again
4. Measure wall-clock time: full refresh vs incremental

## Files to Modify

| File | Change |
|------|--------|
| `models/inputs.yaml` | Add `is_append_only: true` + `is_event_stream: true` to 8 inputs |
| `models/profiles.yaml` | Possibly add `merge:` to count vars (if PB doesn't auto-infer) |

## What's Next

See `plan-a-v2-fewdays-win.md` for:
- Phase 1: Active section tables for all CDC inputs (dedup to current state, entity vars read from deduplicated tables)
- Phase 2: Decompose mutable views to base tables
- Phase 3: Generalize active section pattern across the project

See `plan-d-sdk-cdc-merge.md` for the PB SDK feature (`override_channel`) that would auto-generate active section tables from input contracts, eliminating hand-written SQL models.
