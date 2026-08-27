---
name: postgres-stocki
description: Use when working in StockiAPP and the task requires safe Postgres inspection of schema or data, validating tenant-scoped tables or migrations, or checking credit_sales, credit_installments, audit_events, customers, product_loans, or ownership-sensitive product data without destructive changes.
---

# Postgres Stocki

This skill is repo-scoped for StockiAPP. Use it only for safe inspection of Postgres schema and data in this repository.

## Scope

Use this skill when the task is about:

- inspecting the real Postgres schema before proposing changes
- validating whether a migration is safe against existing data
- checking tenant integrity in operational tables
- inspecting `credit_sales`, `credit_installments`, `audit_events`, `customers`, `customer_events`, `product_loans`, `invoice*`, `productos`, `unidades`
- inspecting receipt metadata stored in `ventas` and customer links stored in `retomas`
- comparing actual Postgres state against the canonical schema in `main.go`

Do not use this skill to bypass the app API for n8n or agent integrations. In StockiAPP, external integrations must use the API, not direct database access.

## Project Ground Truth

Read these first when the task touches schema or tenancy:

- `AGENTS.md`
- `docs/api.md`
- `README.md` section `Desarrollo local con Postgres`
- `main.go`, especially:
  - `loadDatabaseConfig`
  - `initPostgresDB`
  - `tenantIDFromRequest`
  - `tenantIDFromRequestStrict`
  - `tenantIDFromUser`
  - `tenantIDFromUserStrict`
  - `productVisibilityPredicate`
  - `creditVisibilityPredicate`
  - `ensureLegacyOperationalColumns`
  - `ensureCustomerCRMBase`
  - `ensureProductLoanBase`
  - `repairMissingProductosFromUnits`
  - Postgres `CREATE TABLE IF NOT EXISTS ...` blocks

Canonical backend concepts that must stay stable:

- `product`
- `sale`
- `swap`
- `retoma`
- `quantity`
- `inventory`
- `audit_event`
- `owner_user_id`

Visible labels may vary by business. Do not rename internal concepts because of UI wording.

If the database schema, indexes, tenant rules, or canonical table structure change, update this skill in the same change set so it stays aligned with the repo.

## Safety Rules

This skill is inspection-only by default.

- Prefer `SELECT`, catalog queries, `EXPLAIN`, and aggregate checks.
- Start with schema inspection before suggesting any DDL.
- Use `LIMIT` unless the task explicitly needs full scans.
- Prefer counts and grouped summaries before raw row dumps.
- Do not expose broad sensitive auth data. For `api_keys` and `sessions`, inspect counts/metadata first and avoid printing `token_hash` or session tokens unless explicitly required.
- Never run destructive or mutating SQL without an explicit user request.

Forbidden by default:

- `INSERT`
- `UPDATE`
- `DELETE`
- `TRUNCATE`
- `ALTER`
- `DROP`
- `CREATE`
- `VACUUM`
- `REINDEX`
- `COPY` to or from production-like data

If a task later evolves into a real migration, stop treating it as a pure inspection task and switch back to repo rules in `AGENTS.md`: keep StockiAPP Postgres-only, reuse existing schema/bootstrap helpers where they are still canonical, assume non-empty databases, and use safe defaults.

There is no supported SQLite runtime or in-app SQLite migration path anymore. If beta-era SQLite data still exists, treat the move to Postgres as an external migration prerequisite before running the current binary.

## Tenant And Ownership Rules

StockiAPP is multi-tenant. Database inspection must respect that.

- Operational tables usually carry `tenant_id`; verify it before reasoning about data.
- When inspecting app-visible rows, filter by `tenant_id` first.
- Runtime auth and visibility should fail closed when tenant context is missing. Treat `tenant_id <= 0` in `users`, `sessions`, or `api_keys` as broken legacy data to repair, not as a valid runtime fallback.
- Never recommend API contracts that require manual `tenant_id` in payloads, query params, or ad hoc headers.
- Ownership is evaluated inside the resolved tenant. `productos.owner_user_id` is sensitive.
- Product visibility is not only tenant-based:
  - admin can see all products in the tenant
  - `owner_user_id IS NULL` means public inside the tenant
  - non-admin product and credit analysis may require joining through `productos`

When checking credits, mirror the code's tenant-safe joins:

```sql
LEFT JOIN productos p ON p.sku = cs.product_id AND p.tenant_id = cs.tenant_id
LEFT JOIN customers c ON c.id = cs.customer_id AND c.tenant_id = cs.tenant_id
LEFT JOIN credit_installments ci ON ci.tenant_id = cs.tenant_id AND ci.credit_sale_id = cs.id
```

Do not assume `product_id` is always present in credits; `cash_loan` flows allow `product_id` to be null.

## Product Identity Rules

StockiAPP uses two product identifiers with different roles:

- `productos.id`: visible product ID, tenant-scoped, used in routes and API payloads
- `productos.sku`: internal stable identifier, used for operational persistence and historical references

When inspecting history tables, expect internal `sku` values in persistence columns such as:

- `ventas.producto_id`
- `ventas.receipt_buyer_*` stores the latest generated sale receipt identity snapshot for re-open/print flows
- `retomas.producto_id`
- `retomas.customer_id` links a retoma to `customers.id` when customer data was captured
- `movimientos.producto_id`
- `unidades.producto_id`
- `credit_sales.product_id` for product credits
- `product_loans.product_id`

Do not recommend `sku = ? OR id = ?` lookups for runtime behavior. If legacy repair is needed, isolate it to migration or bootstrap paths and document it explicitly.

For inventory unit reads, the canonical Postgres bootstrap creates:

```sql
CREATE INDEX IF NOT EXISTS idx_unidades_tenant_producto_created_id
ON unidades (tenant_id, producto_id, creado_en, id);
```

This index supports both the aggregate inventory query and the ordered lazy unit-detail query. Keep the tenant predicate first in any replacement query; `producto_id` values are internal SKUs, not visible product IDs.

## Recommended Workflow

1. Confirm Postgres context.
2. Inspect actual schema and indexes.
3. Check target tables for `tenant_id`, defaults, nullability, and key columns.
4. Compare with `main.go` and any migration helper already present.
5. Run tenant-integrity checks before proposing changes.
6. Summarize findings with risks, not just raw query output.

If the task touches seeds or legacy backfills, verify that any reconstruction of missing visible IDs happens only in repair/bootstrap code and never by treating `sku` as the normal visible ID. In the current repo, repairs like `repairMissingProductosFromUnits` belong to bootstrap/migration paths, not to normal API/runtime resolution.

Use local environment values from `.env.local` or exported vars. StockiAPP does not auto-load `.env`.

Recommended session style:

```bash
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -P pager=off
```

Inside `psql`, prefer:

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '5s';
-- inspection queries here
COMMIT;
```

## Schema Inspection Cookbook

List user tables:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
```

Inspect columns for one table:

```sql
SELECT
  ordinal_position,
  column_name,
  data_type,
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'credit_sales'
ORDER BY ordinal_position;
```

Inspect indexes:

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'credit_sales'
ORDER BY indexname;
```

Inspect constraints:

```sql
SELECT conname, contype, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'public.credit_sales'::regclass
ORDER BY conname;
```

Before proposing a new column or table, confirm whether it already exists in:

- Postgres catalog
- `main.go` schema blocks
- `ensureCustomerCRMBase`
- `ensureProductLoanBase`
- existing migration helpers such as `migrateCreditTablesForCashLoans`

## Tenant Integrity Checks

Check which operational tables are tenant-scoped:

- `productos`
- `ventas`
- `retomas`
- `unidades`
- `movimientos`
- `users`
- `sessions`
- `api_keys`
- `business_settings`
- `business_lines`
- `business_locations`
- `payment_methods`
- `movement_settings`
- `audit_events`
- `credit_sales`
- `credit_installments`
- `customers`
- `customer_events`
- `invoices`
- `invoice_items`
- `product_loans`
- `product_loan_units`

`business_settings` is tenant-scoped and also stores branding and optional contact fields used by labels: `contact_phone`, `contact_email`, and `social_media`. It also stores `default_label_profile_id`, which selects the tenant's operational label profile. Existing tenants must retain safe defaults when these columns are introduced.

`label_profiles` is tenant-scoped. It stores structured physical label layout (`label_width_mm`, `label_height_mm`, `columns`, `column_gap_mm`) and the enabled visible fields (`show_*`). It must never be resolved across tenants; the initial safe profiles are created lazily per tenant by the application.

Quick count by tenant:

```sql
SELECT tenant_id, COUNT(*) AS rows
FROM credit_sales
GROUP BY tenant_id
ORDER BY tenant_id;
```

Cross-tenant customer link check:

```sql
SELECT COUNT(*) AS broken_links
FROM credit_sales cs
JOIN customers c ON c.id = cs.customer_id
WHERE cs.customer_id IS NOT NULL
  AND c.tenant_id <> cs.tenant_id;
```

Cross-tenant installment link check:

```sql
SELECT COUNT(*) AS broken_links
FROM credit_installments ci
JOIN credit_sales cs ON cs.id = ci.credit_sale_id
WHERE ci.credit_sale_id IS NOT NULL
  AND cs.tenant_id <> ci.tenant_id;
```

Ownership-sensitive products inside one tenant:

```sql
SELECT
  owner_user_id,
  COUNT(*) AS products
FROM productos
WHERE tenant_id = <tenant_id>
GROUP BY owner_user_id
ORDER BY owner_user_id NULLS FIRST;
```

## Credits And Audit Checks

Inspect recent credits with payment rollup:

```sql
SELECT
  cs.id,
  cs.tenant_id,
  cs.kind,
  cs.product_id,
  cs.customer_id,
  cs.status,
  cs.installments_total,
  cs.installments_paid,
  cs.installment_value,
  COUNT(ci.id) AS installment_rows,
  COALESCE(SUM(ci.amount_paid), 0) AS total_paid
FROM credit_sales cs
LEFT JOIN credit_installments ci
  ON ci.tenant_id = cs.tenant_id
 AND ci.credit_sale_id = cs.id
WHERE cs.tenant_id = <tenant_id>
GROUP BY
  cs.id, cs.tenant_id, cs.kind, cs.product_id, cs.customer_id, cs.status,
  cs.installments_total, cs.installments_paid, cs.installment_value
ORDER BY cs.id DESC
LIMIT 50;
```

Inspect audit trail for credits:

```sql
SELECT
  created_at,
  event_type,
  entity_type,
  entity_id,
  source
FROM audit_events
WHERE tenant_id = <tenant_id>
  AND entity_type = 'credit_sale'
ORDER BY created_at DESC
LIMIT 50;
```

If `payload_json` is relevant, verify its real Postgres type first. Do not assume it is `jsonb` just because a migration script casts it.

## Migration Validation Rules

When validating a migration idea:

- inspect the live column set first
- inspect defaults and nullability
- inspect row counts by tenant
- inspect whether backfill is needed for legacy rows
- inspect related indexes
- verify audit and customer traceability impact
- verify whether the change stays aligned with the current Postgres-only runtime and the real schema

Always report:

- affected tables
- required backfill or default values
- tenant impact
- ownership impact if `productos` is involved
- audit/customer-event impact
- whether the proposal stays aligned with the Postgres-only runtime and deployed schema

## Output Expectations

When using this skill, produce a short factual report:

- what was inspected
- what the schema/data says now
- tenant or ownership risks found
- migration or query implications
- exact files/functions in StockiAPP that the finding must stay aligned with

Do not turn inspection into direct database writes unless the user explicitly asks for that next step.
