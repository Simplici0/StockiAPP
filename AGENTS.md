# AGENTS.md — StockiAPP

## Qué es StockiAPP
StockiAPP es una plataforma operativa ligera, adaptable a distintos negocios, con:

- backend canónico en Go
- UI SSR con templates
- API interna
- auditoría
- integración futura con n8n y agentes de IA
- despliegue real con Caddy/systemd

El objetivo no es crear una app distinta por cliente, sino mantener una sola lógica base y adaptar:
- labels visibles
- configuración del negocio
- features habilitadas

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

### 6. Cambios de esquema
Si se agregan columnas o tablas:
- usar migración o `ensureSchema()`
- no asumir base vacía
- usar defaults seguros para datos existentes

### 7. Auditoría
Acciones operativas importantes deben registrar eventos en `audit_events`.

Valores de `source` esperados:
- web
- api
- n8n
- agent
- manual

No eliminar ni debilitar auditoría sin instrucción explícita.

### 8. Ownership
Si un producto tiene `owner_user_id`:
- solo su dueño y admin lo ven/operan
- admin ve todo
- productos sin `owner_user_id` son públicos

Toda nueva query, handler o endpoint debe respetar esta lógica.

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
- `loadMovementSettings(...)`
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
- `GET /api/inventory`
- `POST /api/inventory/adjust`
- `GET /api/retomas`
- `POST /api/retomas`
- `GET /api/sales/recent`
- `GET /api/sales`
- `POST /api/sales`
- `GET /api/settings/lines`
- `GET /api/settings/owners`

Regla:
- no duplicar endpoints ya existentes
- si un contrato necesita ampliarse para n8n/agentes, ajustar el handler existente antes de crear otro

### 14.2 Auditoría de integraciones
Toda operación relevante vía API debe dejar auditoría consistente.

Eventos que hoy ya forman parte del flujo operativo:
- `product_updated`
- `inventory_adjusted`
- `retoma_registered`
- `sale_registered`

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
