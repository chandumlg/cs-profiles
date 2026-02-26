# Incrementalizing cs-profiles

---

## Where we started

cs-profiles: 407 features, 2 entities (workspace, account), 2 cohorts. Full-scans every table, every run.

In v0, we marked what we could as append-only and added incremental SQL models for event streams. That gave us 29 incremental vars. Everything else was left as full-scan.

In this iteration, we opened up the upstream views to understand what's actually underneath.

---

## What we found: the views

The project consumed 7 opaque views. We traced each back to its base tables:

| View | What it does | Vars |
|------|-------------|------|
| `UNION_UPDATED_MERGED_TICKETS` | Dedup thena ticket creates + updates to latest state | 14 |
| `SALESFORCE_VIEW_TABLE` | Join SF accounts → SF users for TAM/CSM names | 4 |
| `ACCOUNT_VIEW_TABLE` | Join user accounts → plans for org name, plan type | 8 |
| `GONG_CALLS` | Filter SF tasks to calls, dedup by call ID | 16 |
| `CUSTOMER_SUCCESS_CONNECTIONS` | 7-table join: connections × sources × destinations × definitions | 22 |
| `CUSTOMER_LIFETIME_MONTHLY_EVENT_VOLUME` | Passthrough to dbt model | 13 |
| `CUSTOMER_LIFETIME_DAILY_EVENT_VOLUME` | Same dbt model, daily grain | 3 |

**Key finding**: All views except the two dbt-backed ones resolve to Stitch or Salesforce base tables — append-only at the storage layer. The data was already incrementalizable. The views were hiding it.

---

## The core insight: two kinds of inputs

Every input is one of:

| | Event Stream | Record Set (CDC) |
|---|---|---|
| **What each row is** | An independent event | A state change to a mutable record |
| **`count(*)` means** | "How many events happened" | "How many records exist now" |
| **Examples** | `app_tracks`, `app_identifies` | `app_sources`, `sf_account`, all Stitch tables |

Both are append-only at storage (Stitch/Salesforce only INSERT). The difference is business semantics.

**Why this matters for incremental**: On event streams, `merge: sum()` is correct — new events add to old count. On record sets, `merge: sum()` **double-counts** — a source counted in batch 1 appears again in batch 2 when updated.

---

## The solution: active section tables

For record-set inputs, deduplicate to current state **before** aggregating:

```
First run:    full_input → dedup by PK, keep latest → materialize
Incremental:  previous_state UNION delta → dedup by PK → materialize
```

Entity vars read from this deduplicated table. Every aggregation is correct — `count`, `sum`, `count(DISTINCT)`, `avg` — because each record appears exactly once.

`thena_current_state` (shipped in v0) already implements this pattern. We generalize it to all CDC inputs.

---

## Progress

### Shipped (v0): 29 vars

| Model | What it does | Vars |
|-------|-------------|------|
| `recent_app_tracks` | 90-day window over event stream | 6 |
| `recent_app_identifies` | 90-day window over event stream | 9 |
| `thena_current_state` | CDC dedup (active section table) | 14 |

### In progress (hours): +43 vars → 72 total

Mark 8 CDC inputs as `is_append_only`. Add `merge:` to 43 vars using `any_value`/`max` — the only aggregations safe for direct merge on CDC tables.

### Next — Phase 1 (days): +266 vars → 338 total

Create 8 active section tables — one per CDC input:

| Active section table | Input | PK | Vars served |
|---------------------|-------|-----|-------------|
| `sources_current_state` | `app_sources` | `ID` | 6 |
| `destinations_current_state` | `app_destinations` | `ID` | 5 |
| `transformations_current_state` | `app_transformations` | `ID` | 6 |
| `workspaces_current_state` | `app_workspaces` | `ID` | 15 |
| `user_roles_current_state` | `app_user_roles` | `ID` | 2 |
| `tracking_plans_current_state` | `app_tracking_plans` | `ID` | 1 |
| `profiles_current_state` | `app_profiles` | `ID` | 1 |
| `sf_account_current_state` | `sf_account` | `ID` | 35 |

Repoint entity vars to read from these tables. 238 rollup vars become incremental automatically (they inherit from workspace vars).

### Phase 2 (days): +50 vars → 388 total

Decompose the 4 mutable views to base tables. Add active section tables for each.

### Remaining: 16 vars (dbt-backed, not incrementalizable from our side)

---

## Scorecard

| | Before | v0 | + Quick Win | + Phase 1 | + Phase 2 |
|---|---|---|---|---|---|
| **Vars incremental** | 0 | 29 | ~72 | ~338 | ~388 |
| **Vars full-scan** | 407 | ~378 | ~335 | ~69 | ~19 |
| **Active section tables** | 0 | 1 | 1 | 9 | ~13 |
| **PB SDK changes** | — | None | None | None | None |

---

## Future: SDK support (`override_channel`)

The active section tables from Phase 1 are hand-written. A PB SDK feature would auto-generate them:

```yaml
contract:
  is_append_only: true
  override_channel: ID           # PK — rows with same ID override each other
  override_ordering: UPDATEDAT   # latest wins
```

PB auto-generates the dedup model, chooses the right incremental strategy per aggregation type, and entity vars need no `merge:` clause. Eliminates all hand-written SQL models for CDC inputs.

This is the same distinction Flink makes (append vs changelog streams), ksqlDB (STREAM vs TABLE), and dbt (`unique_key` on incremental models).

Design documented in `plan-d-sdk-cdc-merge.md`.

---

## Architecture

```
              Event streams                      Record sets (CDC)
                   │                                    │
         ┌─────────┴──────────┐              ┌──────────┴───────────┐
         │                    │              │                      │
   Bounded window       Direct merge    Active section table   Direct merge
   SQL models           (any_value/     (dedup by PK,          (any_value/
   (recent_app_*)        max only)       keep latest)           max only)
         │                    │              │                      │
         └─────────┬──────────┘              └──────────┬───────────┘
                   │                                    │
                   ▼                                    ▼
         Workspace entity vars ───────────→ workspace_profile (VIEW)
                                                     │
                                               rolledUpWorkspaces
                                                     │
                                                     ▼
         Account entity vars ◄─────────────── rollup vars
                   │
                   ▼
         account_profile (VIEW)
                   │
            ┌──────┴──────┐
      AllCustomers    CurrentCustomers
      Prospects       (cohort)
```

---

## Effort summary

| Phase | Work | Time |
|-------|------|------|
| Quick win | `is_append_only` flags + `merge:` clauses | Hours |
| Phase 1 | 8 active section SQL models, repoint ~74 vars | Days |
| Phase 2 | Decompose 4 views, add active section tables | Days |
| SDK: `override_channel` | Auto-generate active section tables from input contract | Weeks |
