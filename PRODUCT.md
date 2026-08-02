# PRODUCT.md

## Product

StockiAPP es una plataforma operativa ligera y adaptable para negocios que necesitan gestionar inventario, ventas y crédito en un solo lugar. No es una app distinta por cliente: es una sola lógica base con labels, configuración y features habilitadas por negocio (multi-tenant).

## Users

- **Operador del negocio (usuario principal):** dueño o encargado de un comercio (compraventa, tienda con crédito interno) que registra ventas, retomas, cambios, préstamos y cuotas desde una tablet en el mostrador.
- **Admin de plataforma:** gestiona tenants, usuarios y API keys; resetea contraseñas.
- **Agentes de IA / n8n (integradores):** automatizan consultas y operaciones vía API (`/api/*`), nunca tocan la base de datos directamente.

## Jobs

- Registrar y consultar inventario de productos y unidades.
- Registrar ventas, retomas, cambios (`swap`) y préstamos físicos de producto.
- Otorgar créditos de producto y préstamos de dinero, con cuotas y abonos.
- Generar comprobantes de pago y tickets térmicos de venta.
- Consultar clientes y su trazabilidad (`customers`, `customer_events`).
- Exportar datos operativos en CSV por tenant.
- Automatizar operaciones vía API para agentes/n8n.

## Purpose / Positioning

StockiAPP es la capa operativa que un negocio usa todos los días: rápida, tablet-first, SSR, sin fricción. La API estable es la fuente de verdad para automatización; la UI es la capa humana. Aislamiento multi-tenant lógico: un solo backend, muchos negocios.

## Operating Context

- Uso principal en tablet en el mostrador del negocio; también escritorio.
- El operador no quiere complejidad: pantallas cortas, tarjetas, labels configurables por negocio.
- Integraciones (n8n, agentes) consumen la API con API keys tenant-scoped, no cookies.

## Capabilities (confirmadas)

- Inventario, ventas, cambios, retomas, créditos de producto, préstamos de dinero, cuotas/abonos, comprobantes con snapshot inmutable, facturas, clientes con trazabilidad, préstamos físicos de producto, etiquetas, locación por producto, ticket térmico, auditoría operativa, exportación CSV, usuarios multi-tenant, reset de contraseña exclusivo de platform_admin.
- SSR: dashboard, inventario, clientes, admin de usuarios, configuración, auditoría, créditos, préstamos, productos, CSV, facturas, comprobantes, tickets.

## Constraints (durables)

- Backend canónico en Go, monolito en `main.go`, SSR con templates + API interna.
- Postgres únicamente (`DB_DSN` o `DATABASE_URL`). Sin SQLite.
- Conceptos internos canónicos estables: product, sale, swap, retoma, quantity, inventory, audit_event, owner_user_id.
- Multi-tenant por contexto de autenticación; nunca enviar `tenant_id` manual.
- Ownership (`owner_user_id`): dueño y admin ven/operan; sin dueño, público.
- Auditoría obligatoria en operaciones importantes (`audit_events`, con `source`: web, api, n8n, agent, manual).
- Los labels visibles son configurables por negocio; no renombrar el modelo interno.
- Integraciones externas solo vía API, nunca directo a la DB.
- Deploy: Go + Caddy + systemd en VPS; no tocar config de infraestructura.
- Documentación viva de API en `docs/api.md`.

## Terminology

- internos: product, sale, swap, retoma, quantity, inventory, audit_event, owner_user_id, tenant.
- visibles (configurables): pago, recompra, cuotas, crédito, etc.

## Evidence

- `DESIGN.md` documenta el contrato del app shell y sidebar (fuente de verdad: `templates/partials/app_styles.html` y `templates/partials/header.html`).
- `docs/api.md` documenta endpoints y ejemplos curl para n8n/agentes.
- `docs/manual-stockiapp.html` es manual autónomo de uso.

## Platform

web

## Accessibility

- Tablet-first; targets táctiles mínimos 44x44px.
- SSR con header compartido; tema claro/oscuro vía `data-theme`.
- El shell preserva nombres accesibles estables y foco determinista.
