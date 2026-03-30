# AGENTS.md — StockiAPP

## Qué es StockiAPP
StockiAPP es una plataforma operativa ligera, adaptable a distintos negocios, con:

- backend canónico en Go
- UI SSR con templates
- API interna
- auditoría
- integración activa con n8n y agentes de IA vía API
- aislamiento multi-tenant por contexto de autenticación
- despliegue real con Caddy/systemd

El objetivo no es crear una app distinta por cliente, sino mantener una sola lógica base y adaptar:
- labels visibles
- configuración del negocio
- features habilitadas

---

## Estado actual del proyecto

Al día de hoy, StockiAPP ya no es solo inventario. El producto quedó extendido sobre un único backend Go con SSR + API multi-tenant.

Dominios ya implementados:
- inventario de productos y unidades
- ventas
- cambios (`swap`)
- retomas
- créditos de producto (`product_credit`)
- préstamos de dinero (`cash_loan`)
- cuotas y abonos de créditos
- facturas operativas
- clientes (`customers`) con trazabilidad (`customer_events`)
- préstamos físicos de producto (`product_loans`)
- usuarios multi-tenant con `telegram_id`
- etiquetas de producto
- ticket térmico de venta
- locación por producto
- auditoría operativa

UI SSR ya disponible:
- `/dashboard`
- `/inventario`
- `/clientes`
- `/clientes/{id}`
- `/admin/users`
- `/configuracion`
- `/auditoria`
- `/creditos/editados`
- `/prestamos/producto`
- `/prestamos/producto/{id}`
- `/productos/new`
- `/productos/etiquetas`
- `/facturas/nueva`
- `/facturas/{id}`
- `/venta/comprobante`
- `/venta/ticket`

Regla práctica:
- antes de proponer una feature nueva, revisar si el dominio ya existe parcialmente en `main.go`
- priorizar extender la base existente antes que crear tablas, pantallas o contratos paralelos

---

## Principios del producto

### 1. Backend canónico
Los conceptos internos del backend deben mantenerse estables aunque el frontend o el agente usen otros nombres visibles.

Conceptos internos canónicos:
- product
- sale
- swap
- retoma
- quantity
- inventory
- audit_event
- owner_user_id

No renombrar estos conceptos internos sin instrucción explícita.

### 2. Lenguaje visible configurable
El frontend puede mostrar otros términos según el negocio:
- sale → pago
- retoma → recompra
- quantity → cuotas
- product → crédito

Pero esto es solo una capa de presentación. No cambiar el modelo interno por ese motivo.

### 3. Integraciones
n8n y agentes de IA no deben tocar la base de datos directamente.
Toda integración externa debe usar la API interna.

### 3.1 Multi-tenant canónico
StockiAPP ya opera con aislamiento lógico multi-tenant dentro de la misma app.

Reglas:
- el tenant se resuelve por sesión web o API key
- no enviar `tenant_id` manual en payloads, query params ni headers ad hoc
- toda query, loader, escritura y validación nueva debe respetar tenant antes que ownership
- ownership (`owner_user_id`) se evalúa dentro del tenant ya resuelto

### 3.2 Base de datos canónica
StockiAPP opera únicamente sobre Postgres.

Reglas:
- no reintroducir compatibilidad con SQLite
- la configuración esperada es `DB_DSN` o `DATABASE_URL`
- `DB_ENGINE`, si se usa, debe ser `postgres`
- el arranque debe fallar cerrado si falta configuración válida de Postgres
- cualquier cambio de acceso a datos debe mantenerse compatible con Postgres y con el esquema real ya desplegado

---

## Arquitectura operativa actual

### A. Monolito Go con SSR y API en `main.go`
La aplicación sigue un patrón monolítico:
- handlers SSR
- handlers `/api/*`
- helpers de negocio
- migraciones / `ensureSchema()`
- auditoría

No introducir una arquitectura paralela por carpetas o microservicios sin pedido explícito.

### B. SSR como capa operativa humana
La UI principal es SSR con templates Go en `templates/`.

Reglas:
- las nuevas pantallas operativas deben integrarse con `header.html` y `app_styles.html`
- preferir SSR para operación humana antes que SPA o frontend separado
- si una vista reutiliza datos ya disponibles por helper o API interna, colgarse de eso

### C. API como fuente de verdad para agentes
La automatización y agentes consumen la API, no la SSR.

Patrón actual:
- la SSR consulta helpers del backend
- la API expone contratos JSON
- los agentes/n8n deben reutilizar la API ya existente

### D. Render SSR seguro
El proyecto ya tuvo fallos por templates que escribían directo a `http.ResponseWriter`.

Reglas:
- para render SSR usar el helper `renderTemplate(...)` o buffer equivalente
- no volver a `tmpl.ExecuteTemplate(w, ...)` seguido de `http.Error(...)`
- si una página comparte el `header`, debe ser compatible con `CurrentUser` y branding

### E. Header compartido y feature flags visibles
El `header` compartido ya resuelve visibilidad de `CanLoan` y `CanCredit`.

Reglas:
- no duplicar menús por pantalla
- si una vista necesita menú administrativo, usar el `header` compartido
- no asumir que cada `PageData` trae todos los flags manualmente; el `header` ya tiene fallback

---

## Reglas arquitectónicas

### 4. Reutilizar lógica existente
Cuando se creen nuevas acciones o endpoints:
- reutilizar la lógica actual
- no duplicar lógica de ventas, cambios, retomas o ownership
- no escribir directo a la DB saltándose validaciones

### 5. Compatibilidad hacia atrás
Toda tarea debe preservar compatibilidad con:
- productos existentes
- ventas históricas
- cambios históricos
- auditoría existente
- ownership actual
- settings ya implementados
- render SSR ya corregido
- flujos multi-tenant y agent-first ya consolidados

### 6. Cambios de esquema
Si se agregan columnas o tablas:
- usar migración o `ensureSchema()`
- no asumir base vacía
- usar defaults seguros para datos existentes

Tablas operativas ya presentes que deben revisarse antes de crear nuevas:
- `customers`
- `customer_events`
- `credit_sales`
- `credit_installments`
- `invoices`
- `invoice_items`
- `product_loans`
- `product_loan_units`
- `audit_events`
- `users`
- `productos`

### 7. Auditoría
Acciones operativas importantes deben registrar eventos en `audit_events`.

Valores de `source` esperados:
- web
- api
- n8n
- agent
- manual

No eliminar ni debilitar auditoría sin instrucción explícita.

Además:
- si el evento afecta a un cliente, evaluar también `customer_events`
- no abrir sistemas paralelos de timeline si ya existe `customer_events` o `audit_events`

### 8. Ownership
Si un producto tiene `owner_user_id`:
- solo su dueño y admin lo ven/operan
- admin ve todo
- productos sin `owner_user_id` son públicos

Toda nueva query, handler o endpoint debe respetar esta lógica.

### 8.1 Clientes y trazabilidad comercial
El pseudo CRM ya existe y es canónico.

Reglas:
- reutilizar `customers` antes de crear datos de cliente embebidos nuevos
- usar `resolveCustomerForCredit(...)` o helpers equivalentes cuando aplique
- la vista SSR de clientes y la API deben seguir compartiendo la misma base
- si una operación genera relación con cliente, preferir dejar `customer_id` además de snapshots textuales si hacen falta por compatibilidad

---

## Reglas para UI

### 9. Tablet-first
La UI debe priorizar uso en tablet.
Evitar:
- headers muy altos
- layout tipo landing page
- exceso de elementos decorativos
- interacciones frágiles solo desktop

### 10. No romper SSR
Tener cuidado con templates Go:
- `{{if}}`
- `{{range}}`
- `{{template}}`
- `{{define}}`
- `{{end}}`

No introducir sintaxis Go inválida ni mezclarla dentro de strings JS por error.

### 11. No rediseñar por accidente
En tareas funcionales:
- no rehacer toda la interfaz
- no cambiar branding global
- no cambiar layouts compartidos salvo que la tarea lo pida explícitamente

### 11.1 ADN Visual y UX (Sistema de Diseño)
Para mantener una interfaz clara, operativa y no sobrecargada, toda nueva pieza visual debe seguir estas reglas:
- **Simplicidad:** priorizar claridad sobre densidad; si un elemento no aporta a la operación, se elimina.
- **Escala de espaciado 8px:** usar múltiplos de 8 (8, 16, 24, 32) en márgenes y paddings para consistencia.
- **Paleta neutra:** fondo general gris muy claro (`#f9fafb`), contenedores en blanco, texto en tonos oscuros neutros.
- **Color de acento único:** un solo color de acento (ej. indigo o esmeralda) reservado para CTAs principales.
- **Touch targets:** diseño tablet-first con áreas interactivas mínimas de 44x44px.
- **Esquinas suaves:** radios de `0.5rem` (~8px) en tarjetas y botones.
- **Jerarquía visual:** un solo título claro por pantalla y uso de tarjetas para agrupar información relacionada.

---

## Reglas para API

### 12. API general vs API agente
Hay dos capas posibles:
- `/api/*` → API general
- `/api/agent/*` → endpoints simplificados para agente/n8n

No reemplazar la API general cuando se creen endpoints para agente. Complementarla.

### 13. JSON consistente
Todos los endpoints `/api/*` deben:
- responder JSON
- no devolver HTML
- usar errores claros
- usar códigos HTTP adecuados

Patrón real del proyecto:
- la API vive principalmente en `main.go`
- no crear una arquitectura paralela `/api/...` separada si la app actual no la usa
- al extender endpoints existentes, seguir el mismo estilo de `mux.HandleFunc(...)`, switch por método y helpers compartidos

Helpers canónicos a reutilizar:
- `writeAPIJSON(...)`
- `writeAPIError(...)`
- `withAPIAuditMetadata(...)`
- `productVisibilityPredicate(...)`
- `productAccessibleByID(...)`
- `availableCountsByProduct(...)`
- `tenantIDFromRequest(...)`
- `tenantIDFromUser(...)`
- `loadMovementSettingsForTenant(...)`
- `movementEnabled(...)`
- `logAuditEvent(...)`

### 14. Auth de integraciones
Las integraciones externas deben usar:
- sesión web solo temporalmente en desarrollo, o
- Bearer token / API key para producción

No asumir cookies como solución final para n8n.

### 14.1 Endpoints ya consolidados
Antes de proponer endpoints nuevos, revisar si ya existe soporte real para el caso de uso en `main.go`.

Endpoints generales ya disponibles:
- `GET /api/health`
- `GET /api/settings/business`
- `GET /api/products`
- `GET /api/products/search`
- `POST /api/products`
- `GET /api/inventory`
- `POST /api/inventory/adjust`
- `GET /api/users`
- `POST /api/users`
- `GET /api/users/{id}`
- `PUT /api/users/{id}`
- `PATCH /api/users/{id}`
- `POST /api/users/{id}/password`
- `POST /api/users/{id}/toggle`
- `GET /api/customers`
- `POST /api/customers`
- `GET /api/customers/{id}`
- `GET /api/customers/{id}/events`
- `GET /api/retomas`
- `POST /api/retomas`
- `GET /api/sales/recent`
- `GET /api/sales`
- `POST /api/sales`
- `GET /api/credits`
- `GET /api/credits/{id}`
- `PUT /api/credits/{id}`
- `PATCH /api/credits/{id}`
- `GET /api/credits/{id}/history`
- `GET /api/credits/edited`
- `POST /api/credits`
- `POST /api/credits/installments`
- `GET /api/invoices`
- `POST /api/invoices`
- `GET /api/invoices/{id}`
- `GET /api/settings/lines`
- `GET /api/settings/owners`

Regla:
- no duplicar endpoints ya existentes
- si un contrato necesita ampliarse para n8n/agentes, ajustar el handler existente antes de crear otro

Endpoints agente ya disponibles:
- `GET /api/agent/business`
- `GET /api/agent/customers/search`
- `POST /api/agent/credits`
- `POST /api/agent/invoices`
- `GET /api/agent/products/search`
- `GET /api/agent/products/price`
- `GET /api/agent/inventory`

### 14.2 Auditoría de integraciones
Toda operación relevante vía API debe dejar auditoría consistente.

Eventos que hoy ya forman parte del flujo operativo:
- `product_updated`
- `inventory_adjusted`
- `retoma_registered`
- `sale_registered`
- `credit_sale_created`
- `credit_installment_paid`
- `sale_receipt_generated`
- `invoice_created`
- `customer_created`
- `customer_updated`
- `product_loan_created`
- `product_loan_closed`
- `credit_sale_updated`
- `tenant_created`
- `tenant_updated`
- `tenant_activated`
- `tenant_deactivated`
- `tenant_initial_api_key_rotated`

Regla:
- para llamadas API usar `source = "api"` salvo que exista una capa explícita `n8n` o `agent`
- incluir metadata con `withAPIAuditMetadata(...)`
- no registrar auditoría manual ad hoc si ya existe helper o patrón equivalente

### 14.3 Contratos para n8n y agentes
Cuando una tarea esté orientada a automatización:
- priorizar payloads simples, explícitos y compatibles con `HttpRequest`
- mantener nombres canónicos internos aunque la salida exponga aliases más cómodos
- documentar ejemplos de `curl` con la URL oficial de producción
- asumir `https://login.stockiapp.co` como base pública documentada mientras no se indique otra

### 14.4 Documentación viva
La fuente operativa para integraciones es `docs/api.md`.

Reglas:
- si cambias o agregas endpoints API, actualiza `docs/api.md` en la misma tarea
- incluir ejemplos `curl` orientados a producción/n8n
- aclarar diferencias entre endpoints parecidos, por ejemplo listados compactos vs listados completos
- documentar filtros, validaciones, campos opcionales y supuestos de compatibilidad
- mantener explícito el comportamiento multi-tenant y la regla de no enviar `tenant_id` manual

### 14.5 API keys y tenants
Las API keys ya son tenant-scoped y existe concepto de API key inicial por tenant.

Reglas:
- no asumir una sola API key global del sistema
- cualquier tarea de gestión de keys debe respetar el tenant resuelto
- la key inicial del tenant se gestiona desde configuración de empresas / tenants
- no romper la rotación ni el naming reservado de keys iniciales sin instrucción explícita

---

## Reglas para tareas

### 15. Una responsabilidad por task
Cada task debe hacer una sola cosa.
Evitar mezclar en una sola tarea:
- UI
- lógica de negocio
- API
- deploy
- refactor masivo

### 16. No sobre-implementar
Si una tarea pide:
- crédito básico
- cuotas básicas
- labels visibles
no convertir eso en:
- módulo financiero completo
- motor de reglas complejo
- rediseño total del dominio

### 17. Cambios acotados
Preferir cambios pequeños y seguros.
No reestructurar `main.go` o todos los templates sin necesidad clara.

### 18. Criterios de aceptación
Toda tarea debe, cuando sea posible, incluir:
- objetivo claro
- compatibilidad hacia atrás
- criterios de aceptación
- prueba manual mínima

### 19. Si la tarea es visual
Cuando el objetivo sea solo UI:
- limitar cambios a templates, estilos y jerarquía visual
- no tocar handlers, queries, validaciones, auditoría ni contratos API
- preservar tablet-first y SSR
- mejorar claridad operativa antes que “embellecer” de forma gratuita

Regla práctica:
- una mejora visual puede reorganizar bloques, headers, contenedores, spacing y énfasis
- si una mejora exige cambiar lógica o backend, dejarla como recomendación aparte y no implementarla en esa tarea

### 19.1 Widgets reutilizables ya introducidos
Antes de crear otra interacción similar, revisar si ya existe una base reutilizable.

Ejemplos actuales:
- lookup de clientes en UI sobre `GET /api/customers`
- `renderTemplate(...)` para SSR segura
- `header.html` como navegación compartida
- `app_styles.html` como base visual común
- `customer_lookup.js` como widget de reutilización de cliente

### 20. Activos comunicativos
El proyecto puede incluir piezas HTML autónomas de comunicación o manuales de uso fuera del flujo principal.

Reglas:
- si el archivo es solo comunicativo, mantenerlo desacoplado de la app
- no mezclarlo con SSR, handlers ni navegación productiva salvo pedido explícito
- el manual autónomo actual vive en `docs/manual-stockiapp.html`
- estos archivos no sustituyen la documentación técnica de `docs/api.md`

---

## Límites de seguridad para agentes de código

Sin instrucción explícita, no hacer:
- borrado masivo de productos o datos
- refactors grandes de arquitectura
- cambio de motor de base de datos
- cambios de deploy en VPS
- cambios en Caddy/systemd/GitHub Actions
- eliminación de auditoría
- eliminación de ownership
- renombrado de conceptos canónicos del backend
- exponer `tenant_id` manual en la API como atajo para integraciones

---

## Deployment

StockiAPP se despliega en VPS con:
- Go binario
- Caddy
- systemd
- base de datos persistente

No tocar:
- configuración de Caddy
- service files
- GitHub Actions de deploy
- rutas de producción
- conexión productiva al VPS de Postgres

salvo que la tarea lo pida explícitamente.

---

## Regla final
StockiAPP debe evolucionar como:
- una sola plataforma
- una sola lógica base
- labels configurables por negocio
- features configurables por negocio
- API estable para automatización
- backend canónico y consistente

Si una decisión simplifica frontend/agente pero complica o fragmenta el backend, priorizar el backend canónico.
