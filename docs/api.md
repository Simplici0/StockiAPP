# API interna de Stocki App

Esta es la referencia oficial de la API multi-tenant de Stocki App para automatizaciones, agentes y workflows de `n8n`.

La regla operativa es simple:
- cada negocio usa su propia API key
- el backend resuelve el `tenant` automáticamente
- no se envía `tenant_id` manual ni en auth ni en payloads
- si cambia solo el `APIKEY`, cambia solo el negocio al que apunta el workflow

## Runtime Y Base De Datos

StockiAPP opera exclusivamente sobre Postgres.

Reglas operativas:

- la configuración válida es `DATABASE_URL` o `DB_DSN`
- `DB_ENGINE`, si se define, debe ser `postgres`
- no existe `DB_PATH` ni soporte runtime para SQLite
- la API y los ejemplos de este documento asumen un backend ya conectado a Postgres

Nota de transición:

- la etapa beta de compatibilidad dual terminó
- si tienes datos legacy de SQLite, debes migrarlos a Postgres antes de usar el binario actual
- las reparaciones legacy que siguen en el bootstrap actual solo normalizan datos ya presentes en Postgres; no ejecutan una migración desde SQLite

## Modelo De Acceso

Stocki App usa una sola app, una sola base de datos y aislamiento lógico por `tenant_id`.

Para agentes, el canal oficial es:

```http
Authorization: Bearer <APIKEY>
```

Reglas importantes:
- `Bearer` tiene prioridad sobre la sesión web cuando ambos llegan a `/api/*`
- si no llega `Bearer`, `/api/*` todavía puede usar sesión por compatibilidad
- las requests mutantes a `/api/*` autenticadas por cookie/sesión pasan validación same-origin (`Origin` o `Referer`)
- para automatizaciones nuevas, el contrato oficial es `Bearer`
- una API key autentica un principal operativo tenant-scoped, no un admin implícito del tenant
- si el tenant está inactivo, la API rechaza la request aunque el token exista

## API Key Para Agentes

Las API keys se crean y regeneran desde `Configuración > Empresas / tenants`.

Reglas de la key:
- el token plano solo se muestra al crearse o regenerarse
- en base de datos solo queda `token_hash`
- la key ya queda asociada al `tenant_id` correcto
- no hay que mandar `tenant_id` manual
- un workflow por negocio debe usar una key por negocio
- si la key está inactiva, deja de autenticar de inmediato
- por seguridad, la key no abre endpoints administrativos sensibles por defecto

Rutas administrativas que requieren sesión admin del tenant:
- gestión de usuarios en `/api/users*`
- owners asignables en `/api/settings/owners`
- cambio de ID visible de producto en `/api/products/{id}`
- reporte `/api/credits/edited`

Uso recomendado en `n8n`:
- mismo workflow base
- cambia solo el `APIKEY`
- valida el tenant con `GET /api/health` o `GET /api/agent/business`

Headers recomendados para `HttpRequest`:

```http
Authorization: Bearer TU_TOKEN
Accept: application/json
Content-Type: application/json
```

## Headers De Contexto

Cuando la request está autenticada, la API expone estos headers:

- `X-Stocki-Tenant-ID`
- `X-Stocki-Tenant-Slug`
- `X-Stocki-Auth-Mode`
- `X-Stocki-Integration-Name`

Uso práctico:
- confirmar qué tenant resolvió realmente el token
- depurar workflows de `n8n`
- auditar integraciones por negocio

## Flujo Recomendado Para `n8n`

1. Verifica `GET /api/health`.
2. Inicializa contexto con `GET /api/agent/business`.
3. Consulta catálogo con `GET /api/agent/products/search` o `GET /api/agent/inventory`.
4. Ejecuta la operación de negocio.
5. Usa `auth_mode` e `integration_name` para trazabilidad.

## Formato De Respuesta

Respuesta exitosa típica:

```json
{
  "ok": true
}
```

Respuesta de error típica:

```json
{
  "ok": false,
  "error": "Mensaje de error",
  "fields": {
    "campo": "Detalle opcional"
  }
}
```

`fields` aparece cuando hay validación por campo.

## Identidad De Producto

StockiAPP usa dos identificadores distintos:

- `id`: identificador visible del producto, scoped por tenant, usado en rutas y payloads API
- `sku`: identificador interno estable, usado para persistencia operativa e histórica

Reglas del contrato:

- para entrada humana y API usa `id`
- la API remapea en salida los históricos persistidos por `sku` para devolver `product_id` visible cuando el producto sigue resolviendo dentro del tenant
- columnas históricas como `ventas.producto_id`, `retomas.producto_id`, `credit_sales.product_id` o `product_loans.product_id` siguen persistiendo `sku` interno
- el `sku` solo se expone cuando el contrato lo hace explícito, por ejemplo en respuestas de administración de producto o en el endpoint legado `GET /api/productos/precio?sku=...`
- si una fila histórica no puede remapearse porque el catálogo ya no resuelve ese producto, la salida conserva el valor persistido como fallback legado visible

## Salud

### `GET /api/health`

Healthcheck JSON de la API.

Requiere una API key o una sesión válidas.

Ejemplo:

```bash
curl -X GET "https://login.stockiapp.co/api/health" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json"
```

Respuesta:

```json
{
  "ok": true,
  "service": "stocki-app",
  "auth_mode": "api_key",
  "tenant": {
    "id": 2,
    "slug": "tenant-dos",
    "name": "Tenant Dos"
  }
}
```

Uso recomendado:
- comprobar que el token sigue activo
- validar que el workflow apunta al tenant esperado
- confirmar si la request entra por `api_key` o `session`

## Flujo Recomendado Para Agentes

Este es el flujo mínimo recomendado para `n8n`:

1. Llama `GET /api/health` para validar autenticación y tenant.
2. Llama `GET /api/agent/business` para leer contexto del negocio.
3. Busca cliente con `GET /api/agent/customers/search?q=...`.
4. Si el cliente existe, reutiliza `customer_id`.
5. Si no existe, crea el cliente con `POST /api/customers`.
6. Crea el movimiento comercial con `POST /api/agent/credits` o `POST /api/credits`.
7. Si necesitas documento operativo, genera factura con `POST /api/agent/invoices` o `POST /api/invoices`.
8. Registra cuotas o abonos con `POST /api/credits/installments`.

Regla práctica:
- `product_credit` usa producto
- `cash_loan` no usa producto ni inventario
- no envíes `tenant_id`; el tenant se resuelve por `Authorization: Bearer <APIKEY>`

## Configuración De Negocio

### `GET /api/settings/business`

Devuelve la configuración visible del negocio para el tenant autenticado.

Notas:
- no requiere `tenant_id` manual
- la respuesta ya viene tenant-aware
- `logo_path` se devuelve por compatibilidad

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  https://login.stockiapp.co/api/settings/business
```

Respuesta:

```json
{
  "ok": true,
  "settings": {
    "business_name": "Tenant Dos Brand",
    "logo_path": "/static/logo.png",
    "contact_phone": "+57 300 123 4567",
    "contact_email": "ventas@tenantdos.co",
    "social_media": "Instagram @tenantdos",
    "primary_color": "#112233",
    "currency": "USD",
    "date_format": "2006-01-02"
  },
  "tenant": {
    "id": 2,
    "slug": "tenant-dos",
    "name": "Tenant Dos"
  }
}
```

### `GET /api/settings/lines`

Devuelve las líneas de negocio del tenant autenticado.

Por defecto devuelve solo las activas.
Si la request viene de un admin por sesión web, puede incluir inactivas con `?include_inactive=true`.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/settings/lines?include_inactive=true"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 1,
      "name": "Farmacia",
      "active": true,
      "created_at": "2026-03-20",
      "updated_at": "2026-03-20"
    }
  ]
}
```

### `GET /api/settings/owners`

Devuelve los usuarios asignables del tenant autenticado para `owner_user_id`.

Disponible para administración tenant-scoped por sesión web admin o por API key operativa tenant-scoped.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  https://login.stockiapp.co/api/settings/owners
```

Respuesta:

```json
{
  "ok": true,
  "count": 2,
  "items": [
    {
      "id": 2,
      "username": "maria"
    },
    {
      "id": 3,
      "username": "carlos"
    }
  ]
}
```

## Productos E Inventario

### `GET /api/products`

Lista productos visibles para el usuario autenticado.

Cada item incluye también `location`, `talla_requerida` y `talla` cuando el producto tiene esos datos operativos registrados.

### `GET /api/products/search?q=`

Busca productos visibles por `id`, nombre, línea, locación o deudor.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/products/search?q=crema"
```

### `POST /api/products`

Crea un producto y sus unidades iniciales.

Solo admin.

Notas:
- `id` es el identificador visible del producto y queda scoped por tenant
- el backend genera además un `sku` interno global para referencias operativas
- `sku` en el payload se conserva solo como alias legado del `id` visible
- para integraciones nuevas, usa `id` y no envíes `sku`
- si envías `id` y `sku` con valores distintos, la API responde `400`

Payload:

```json
{
  "name": "Producto API",
  "line": "Farmacia",
  "location": "Estante A-03",
  "talla": "M",
  "owner_user_id": 2,
  "quantity": 5,
  "sale_price": 25000,
  "retoma_enabled": true,
  "retoma_price": 12000,
  "aplica_caducidad": false,
  "fecha_caducidad": ""
}
```

Reglas:
- `name` y `line` son obligatorios
- `id` es opcional; si no lo envías, el backend genera un `id` visible tenant-scoped
- `location` es opcional
- `talla` es opcional; si contiene un valor, el backend entiende automáticamente que el producto requiere talla
- `talla_requerida` se conserva como campo legado compatible: `true` sigue exigiendo una talla aunque `talla` esté vacía
- si `talla` está vacía y no se envía `talla_requerida=true`, el producto queda sin requisito de talla
- `quantity` debe ser mayor a `0`
- `owner_user_id` es opcional
- `retoma_enabled=true` exige `retoma_price` válido
- `aplica_caducidad=true` exige `fecha_caducidad` en `YYYY-MM-DD`

### `PUT /api/products/{id}` o `PATCH /api/products/{id}`

Actualiza el `id` visible de un producto existente.

Disponible para administración tenant-scoped por sesión web admin o por API key operativa tenant-scoped.

Payload:

```json
{
  "id": "P-900"
}
```

También acepta `new_id` o `sku` como alias de compatibilidad transicional, pero el campo recomendado para integraciones nuevas es `id`.

Reglas:
- la ruta recibe el `id` visible actual del producto
- el nuevo `id` es obligatorio
- no puede colisionar con otro `id` visible ni con un `sku` interno ya existente dentro del tenant
- el cambio no renombra referencias operativas históricas
- el `sku` interno del producto se mantiene estable
- si mandas `sku` en el payload, se interpreta como alias del nuevo `id` visible, no como cambio del `sku` interno
- si mandas `id` y `sku` con valores distintos en el payload, la API responde `400`

Ejemplo:

```bash
curl -X PATCH "https://login.stockiapp.co/api/products/P-001" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "P-900"
  }'
```

Respuesta:

```json
{
  "ok": true,
  "previous_id": "P-001",
  "sku": "SKU-000127",
  "id": "P-900",
  "message": "ID de producto actualizado correctamente."
}
```

### `GET /api/inventory`

Devuelve el inventario agregado del tenant autenticado.

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": "P-001",
      "name": "Crema corporal",
      "line": "Farmacia",
      "location": "Estante A-03",
      "available": 3,
      "reserved": 0,
      "swapped": 0,
      "damaged": 0,
      "sale_price": 25000,
      "retoma_enabled": true,
      "retoma_price": 12000,
      "owner_user_id": 2
    }
  ]
}
```

### `POST /api/inventory/adjust`

Ajusta inventario y, si aplica, precio o retoma del producto.

Disponible para personal autorizado del tenant. Con sesión web requiere `staff`/`admin`; con API key operativa tenant-scoped también está permitido.

Payload:

```json
{
  "product_id": "P-001",
  "target_quantity": 10,
  "sale_price": 25000,
  "retoma_enabled": true,
  "retoma_price": 12000,
  "notes": "Ajuste desde n8n"
}
```

Respuesta:

```json
{
  "ok": true,
  "product_id": "P-001",
  "previous_quantity": 8,
  "current_quantity": 10,
  "delta": 2,
  "message": "Inventario ajustado correctamente."
}
```

## Retomas

### `GET /api/retomas`

Lista retomas visibles para el usuario autenticado.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/retomas?q=usado"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 1,
      "fecha": "2026-03-22",
      "product_id": "P-001",
      "product_name": "Crema corporal",
      "customer_id": 4,
      "customer_name": "Cliente Retoma",
      "quantity": 1,
      "value_received": 12000,
      "received_state": "Usado",
      "published_to_stock": true,
      "final_sale_price": 25000,
      "notes": "Retoma desde n8n"
    }
  ]
}
```

### `POST /api/retomas`

Registra una retoma.

Solo personal autorizado.

Payload:

```json
{
  "product_id": "P-001",
  "quantity": 1,
  "value_received": 12000,
  "received_state": "Usado",
  "publish_to_stock": true,
  "final_sale_price": 25000,
  "notes": "Retoma desde n8n",
  "customer_name": "Cliente Retoma",
  "customer_phone": "3001234567",
  "customer_document_type": "CC",
  "customer_document_number": "99887766",
  "customer_city": "Bogota"
}
```

Notas:
- los campos de cliente son opcionales para compatibilidad
- si envías `customer_id`, la retoma se vincula a ese cliente dentro del tenant
- si envías identidad de cliente sin `customer_id`, la API crea o reutiliza el cliente por `document_type + document_number`

Respuesta:

```json
{
  "ok": true,
  "retoma_id": 10,
  "product_id": "P-001",
  "product_name": "Crema corporal",
  "customer_id": 4,
  "quantity": 1,
  "value_received": 12000,
  "received_state": "Usado",
  "published_to_stock": true,
  "units_created": 1,
  "message": "Retoma registrada correctamente."
}
```

## Ventas

### `GET /api/sales/recent`

Lista ventas recientes del tenant autenticado.

### `GET /api/sales`

Lista ventas filtrables por `q`, `from` y `to`.

Reglas:
- `from` y `to` usan formato `YYYY-MM-DD`
- el filtro respeta visibilidad por tenant y ownership
- `product_id` se devuelve como `id` visible aunque `ventas.producto_id` persista `sku` interno

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/sales?q=crema&from=2026-03-01&to=2026-03-31"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 1,
      "fecha": "2026-03-22",
      "product_id": "P-001",
      "product_name": "Crema corporal",
      "quantity": 2,
      "sale_price": 25000,
      "channel": "WhatsApp",
      "sold_by": "maria",
      "notes": "Venta desde n8n",
      "payment_method": "Efectivo"
    }
  ]
}
```

### `POST /api/sales`

Registra una venta normal.

Payload compatible:

```json
{
  "product_id": "P-001",
  "quantity": 2,
  "payment_method": "Efectivo",
  "sale_price": 25000,
  "channel": "WhatsApp",
  "sold_by": "maria",
  "notes": "Venta desde n8n"
}
```

Notas:
- `quantity` puede venir como entero o usar el valor por defecto `1`
- `sale_price`, `unit_price` y `total` tienen lógica de compatibilidad
- `payment_method` debe existir y estar activo para el tenant

Respuesta:

```json
{
  "ok": true,
  "sale_id": 100,
  "product_id": "P-001",
  "product_name": "Crema corporal",
  "quantity": 2,
  "sale_price": 25000,
  "receipt_url": "/venta/comprobante?sale_id=100",
  "receipt_download_url": "/venta/comprobante?sale_id=100&download=1",
  "thermal_ticket_url": "/venta/ticket?sale_id=100",
  "invoice_create_url": "/facturas/nueva?sale_id=100",
  "message": "Venta registrada correctamente."
}
```

Notas de comprobante:
- `receipt_url` abre la vista estándar del comprobante
- `thermal_ticket_url` abre la versión térmica
- la primera vez que falten `buyer_name` o `buyer_document`, la vista los pide antes de renderizar
- una vez generados, el último nombre, documento, formato y usuario quedan persistidos en `ventas` para reabrir o reimprimir sin volver a capturarlos

## Facturas Operativas

Las facturas de esta fase son documentos operativos, no facturación electrónica.

Reglas:
- no se envía `tenant_id`
- el tenant se resuelve por `Bearer/API key`
- una factura se vincula a una `sale_id` o a una `credit_sale_id`
- no crea inventario, no toca stock y no reemplaza ventas o créditos
- si ya existe una factura para esa venta o crédito, la API reutiliza la existente

### `GET /api/invoices`

Lista facturas visibles del tenant autenticado.

Filtros soportados:
- `q`
- `date_from`
- `date_to`
- `limit`

`q` busca por:
- `invoice_number`
- `customer_name`
- `customer_document_number`

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/invoices?q=99887766&date_from=2026-03-01&date_to=2026-03-31"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 12,
      "invoice_number": "FAC-20260325-000012",
      "source_type": "sale",
      "source_label": "Venta",
      "sale_id": 100,
      "credit_sale_id": 0,
      "customer_name": "Cliente Factura Venta",
      "customer_document": "99887766",
      "total": 45000,
      "status": "issued",
      "status_label": "Factura emitida",
      "created_at": "2026-03-25 16:10",
      "view_url": "/facturas/12",
      "thermal_ticket_url": "/facturas/12?paper=58mm"
    }
  ]
}
```

### `POST /api/invoices`

Crea una factura operativa a partir de una venta o de un crédito existente.

Reglas:
- debes enviar `sale_id` o `credit_sale_id`
- no envíes ambos al tiempo
- para facturas basadas en venta debes enviar cliente
- para facturas basadas en crédito puedes omitir cliente si el crédito ya tiene cliente asociado
- si la factura ya existe para esa referencia, la API responde `200` con `created = false`
- `view_url` abre la factura operativa por defecto
- `thermal_ticket_url` devuelve la misma factura con ancho de papel listo para impresión térmica
- `invoice.items[*].product_id` conserva el `id` visible usado al generar la factura, aunque el origen operativo persista `sku` interno

Factura sobre venta:

```bash
curl -X POST "https://login.stockiapp.co/api/invoices" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "sale_id": 100,
    "customer_name": "Cliente Factura Venta",
    "customer_phone": "3001234567",
    "customer_document_type": "CC",
    "customer_document_number": "99887766",
    "customer_address": "Calle 10 # 1-20",
    "customer_city": "Bogota",
    "notes": "Factura operativa de venta"
  }'
```

Factura sobre crédito:

```bash
curl -X POST "https://login.stockiapp.co/api/invoices" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "credit_sale_id": 10,
    "notes": "Factura operativa del crédito"
  }'
```

Compatibilidad:
- también se aceptan `debtor_name`, `debtor_phone`, `debtor_document_type` y `debtor_document_number` como alias mínimos de cliente

Respuesta:

```json
{
  "ok": true,
  "created": true,
  "message": "Factura generada correctamente.",
  "invoice": {
    "id": 12,
    "invoice_number": "FAC-20260325-000012",
    "source_type": "sale",
    "source_label": "Venta",
    "sale_id": 100,
    "credit_sale_id": 0,
    "customer_id": 4,
    "customer_name": "Cliente Factura Venta",
    "customer_phone": "3001234567",
    "customer_document_type": "CC",
    "customer_document_number": "99887766",
    "customer_address": "Calle 10 # 1-20",
    "customer_city": "Bogota",
    "notes": "Factura operativa de venta",
    "subtotal": 45000,
    "total": 45000,
    "status": "issued",
    "status_label": "Factura emitida",
    "created_at": "2026-03-25 16:10",
    "view_url": "/facturas/12",
    "thermal_ticket_url": "/facturas/12?paper=58mm",
    "items": [
      {
        "product_id": "P-001",
        "description": "Crema corporal",
        "quantity": 1,
        "unit_price": 45000,
        "total": 45000
      }
    ]
  }
}
```

### `GET /api/invoices/{id}`

Devuelve el detalle completo de una factura visible para el tenant autenticado.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/invoices/12"
```

### `POST /api/agent/invoices`

Wrapper orientado a agente/n8n para crear la misma factura operativa sin cambiar la lógica del backend.

Reglas:
- reutiliza exactamente el mismo flujo que `POST /api/invoices`
- si la referencia ya tiene factura, devuelve la existente con `created = false`
- tenant resuelto solo por `Authorization: Bearer <APIKEY>`

Ejemplo mínimo para préstamo o crédito ya existente:

```bash
curl -X POST "https://login.stockiapp.co/api/agent/invoices" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "credit_sale_id": 10,
    "notes": "Factura creada desde agente"
  }'
```

Ejemplo mínimo para venta:

```bash
curl -X POST "https://login.stockiapp.co/api/agent/invoices" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "sale_id": 100,
    "customer_name": "Cliente Factura Venta",
    "customer_document_type": "CC",
    "customer_document_number": "99887766",
    "customer_city": "Bogota"
  }'
```

## Cambios

### `POST /api/swaps`

Registra un cambio entre salida e ingreso.

El payload admite dos modos:

Modo `existing`:

```json
{
  "product_id": "P-001",
  "quantity": 1,
  "persona_del_cambio": "Cliente final",
  "notes": "Cambio desde n8n",
  "incoming_mode": "existing",
  "incoming_existing_id": "P-002",
  "incoming_existing_qty": 1
}
```

Modo `new`:

```json
{
  "product_id": "P-001",
  "quantity": 1,
  "persona_del_cambio": "Cliente final",
  "notes": "Cambio desde n8n",
  "incoming_mode": "new",
  "incoming_new_sku": "P-999",
  "incoming_new_name": "Producto nuevo",
  "incoming_new_line": "Nuevos",
  "incoming_new_qty": 1
}
```

Nota:
- `incoming_new_sku` conserva nombre legado, pero el valor esperado es el `id` visible del producto nuevo; el backend genera su `sku` interno por separado

Respuesta:

```json
{
  "ok": true,
  "product_id": "P-001",
  "incoming_product_id": "P-002",
  "quantity": 1,
  "incoming_quantity": 1,
  "message": "Cambio registrado correctamente."
}
```

## Clientes

### `GET /api/customers`

Lista clientes visibles del tenant autenticado.

Permite búsqueda con `q=` por:
- nombre
- teléfono
- tipo/número de documento
- ciudad

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/customers?q=juan"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 4,
      "name": "Juan Perez",
      "phone": "3001234567",
      "document_type": "CC",
      "document_number": "123456789",
      "address": "Calle 10 # 20-30",
      "city": "Bogota",
      "notes": "Cliente frecuente",
      "created_at": "2026-03-25",
      "updated_at": "2026-03-25",
      "credits_count": 2,
      "units_on_credit": 2,
      "debt_total": 600000,
      "total_paid": 150000,
      "current_debt": 450000,
      "active_credits": 2,
      "last_credit_at": "2026-03-25"
    }
  ]
}
```

### `POST /api/customers`

Crea o reutiliza un cliente del tenant autenticado.

Reglas:
- si ya existe un cliente con el mismo `customer_document_type` + `customer_document_number` dentro del tenant, la API lo reutiliza y actualiza sus datos básicos
- si no existe, crea uno nuevo
- `customer_city` es obligatoria

Payload recomendado:

```json
{
  "customer_name": "Juan Perez",
  "customer_phone": "3001234567",
  "customer_document_type": "CC",
  "customer_document_number": "123456789",
  "customer_address": "Calle 10 # 20-30",
  "customer_city": "Bogota",
  "customer_notes": "Cliente frecuente"
}
```

Compatibilidad:
- `debtor_name` y `debtor_phone` también se aceptan como alias mínimos

Respuesta cuando crea:

```json
{
  "ok": true,
  "created": true,
  "reused": false,
  "customer": {
    "id": 4,
    "name": "Juan Perez"
  },
  "message": "Cliente creado correctamente."
}
```

Respuesta cuando reutiliza/actualiza:

```json
{
  "ok": true,
  "created": false,
  "reused": true,
  "customer": {
    "id": 4,
    "name": "Juan Perez"
  },
  "message": "Cliente actualizado correctamente."
}
```

### `GET /api/customers/{id}`

Devuelve la ficha básica del cliente y un resumen comercial.

Incluye:
- datos base del cliente
- resumen financiero agregado
- créditos recientes asociados

Notas:
- `recent_credits[*].product_id` se devuelve como `id` visible cuando el producto todavía existe en el tenant
- los bloques comerciales embebidos de cliente no exponen `sku` interno salvo fallback legado si el catálogo ya no puede remapear el histórico

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/customers/4"
```

### `GET /api/customers/{id}/events`

Devuelve la trazabilidad resumida del cliente desde `customer_events`.

Notas:
- cuando un crédito se edita, aparece un evento `credit_updated`
- ese evento ahora incluye `changes`, `changed_fields`, `change_count` e `impact`
- esto permite ver el efecto operativo del cambio desde la ficha del cliente sin consultar auditoría global

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/customers/4/events?limit=20"
```

Respuesta por item:

```json
{
  "id": 18,
  "event_type": "credit_payment_recorded",
  "ref_type": "credit_sale",
  "ref_id": "10",
  "amount": 25000,
  "payload": {
    "payment_type": "cuota",
    "current_debt": 250000
  },
  "created_at": "2026-03-25",
  "created_by": "mauro"
}
```

## Usuarios

Estos endpoints reutilizan la misma base compartida que usa `/admin/users` en la app.

Reglas:
- solo `admin` y `platform_admin` pueden operar usuarios por API
- el tenant se resuelve por la sesión admin autenticada
- no se envía `tenant_id` manual
- `telegram_id` está disponible para lectura y edición
- no se puede dejar un tenant sin al menos un admin activo
- solo un `platform_admin` puede crear o editar usuarios `platform_admin`
- las API keys operativas pueden usar `GET /api/users`, pero siguen respondiendo `403` en `/api/users/{id}` y rutas mutantes de usuarios

Nota:
- estas rutas están pensadas para la administración web del tenant; si reproduces requests fuera del browser, reutiliza una sesión admin válida en vez de una API key

### `GET /api/users`

Lista usuarios visibles del tenant autenticado.

Disponible para sesión admin o API key operativa tenant-scoped. La respuesta sigue limitada al tenant autenticado.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/users"
```

Respuesta:

```json
{
  "ok": true,
  "count": 2,
  "items": [
    {
      "id": 7,
      "username": "tenant2.ops",
      "name": "Operador Dos",
      "email": "tenant2.ops@example.com",
      "role": "empleado",
      "is_active": true,
      "tenant_id": 2,
      "created_at": "2026-03-25",
      "telegram_id": "44556677"
    }
  ]
}
```

### `POST /api/users`

Crea un usuario del tenant autenticado usando la misma validación de la UI SSR.

Solo admin por sesión web. Las API keys operativas responden `403`.

Payload mínimo:

```json
{
  "username": "tenant2.ops",
  "password": "OpsSegura123!",
  "role": "empleado",
  "is_active": true
}
```

Payload completo:

```json
{
  "username": "tenant2.ops",
  "name": "Operador Dos",
  "email": "tenant2.ops@example.com",
  "password": "OpsSegura123!",
  "role": "empleado",
  "is_active": true,
  "telegram_id": "44556677"
}
```

Respuesta:

```json
{
  "ok": true,
  "user": {
    "id": 7,
    "username": "tenant2.ops",
    "role": "empleado",
    "is_active": true,
    "telegram_id": "44556677"
  },
  "message": "Usuario creado correctamente."
}
```

### `GET /api/users/{id}`

Devuelve el detalle del usuario dentro del tenant autenticado.

Solo admin por sesión web. Las API keys operativas responden `403`.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/users/7"
```

Respuesta:

```json
{
  "ok": true,
  "user": {
    "id": 7,
    "username": "tenant2.ops",
    "name": "Operador Dos",
    "email": "tenant2.ops@example.com",
    "role": "empleado",
    "is_active": true,
    "tenant_id": 2,
    "created_at": "2026-03-25",
    "telegram_id": "44556677"
  }
}
```

### `PUT /api/users/{id}` o `PATCH /api/users/{id}`

Actualiza el usuario reutilizando exactamente la misma lógica compartida que usa la UI.

Puedes enviar todos los campos o solo los que quieras cambiar:
- `username`
- `name`
- `email`
- `role`
- `is_active`
- `telegram_id`

`PUT` y `PATCH` se comportan igual en este contrato: ambos permiten actualización parcial.

Ejemplo:

```bash
curl -X PATCH "https://login.stockiapp.co/api/users/7" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Operador Dos Actualizado",
    "telegram_id": "88990011",
    "is_active": false
  }'
```

Validación importante:
- si el usuario objetivo es el último admin activo del tenant, la API responde error y no desactiva ni degrada ese usuario

### `POST /api/users/{id}/password`

Actualiza la contraseña del usuario y cierra sus sesiones activas.

Reglas:
- solo una sesión de `platform_admin` puede ejecutar esta operación
- un `admin` de tenant recibe `403`
- las API keys no pueden acceder a rutas sensibles `/api/users/{id}`
- el reset temporal con cambio obligatorio se opera desde la UI global de `/admin/users`

Ejemplo:

```bash
curl -X POST "https://login.stockiapp.co/api/users/7/password" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "password": "NuevaClave123!"
  }'
```

Respuesta:

```json
{
  "ok": true,
  "user_id": 7,
  "message": "Contraseña actualizada correctamente."
}
```

### `POST /api/users/{id}/toggle`

Activa o inactiva un usuario sin borrarlo.

Reglas:
- reutiliza la misma validación compartida de usuarios
- si inactiva al usuario, sus sesiones activas se cierran
- no permite dejar al tenant sin al menos un admin activo
- respeta la restricción sobre usuarios `platform_admin`

Si no envías body, invierte el estado actual.
Si quieres ser explícito, envía `is_active`.

Ejemplo:

```bash
curl -X POST "https://login.stockiapp.co/api/users/7/toggle" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "is_active": false
  }'
```

Respuesta:

```json
{
  "ok": true,
  "user": {
    "id": 7,
    "username": "tenant2.ops",
    "is_active": false
  },
  "message": "Usuario inactivado correctamente."
}
```

## Créditos

### `GET /api/credits`

Lista créditos visibles para el usuario autenticado.

`q=` busca por:
- producto
- tipo de crédito
- nombre del cliente/deudor
- documento
- teléfono
- ciudad

Reglas financieras actuales:
- `debt_total = installments_total * installment_value`
- `total_paid` suma cuotas y abonos
- `current_debt = debt_total - total_paid`
- `product_id` se devuelve como `id` visible; la persistencia interna del crédito sigue usando `credit_sales.product_id` con `sku`

Respuesta por item:

```json
{
  "id": 10,
  "created_at": "2026-03-25",
  "kind": "product_credit",
  "kind_label": "Crédito",
  "product_id": "P-001",
  "product": "Crema corporal",
  "quantity": 1,
  "customer_id": 4,
  "customer_name": "Juan Perez",
  "customer_phone": "3001234567",
  "customer_document_type": "CC",
  "customer_document_number": "123456789",
  "customer_address": "Calle 10 # 20-30",
  "customer_city": "Bogota",
  "customer_notes": "Cliente frecuente",
  "debtor_name": "Juan Perez",
  "debtor_document_type": "CC",
  "debtor_document_number": "123456789",
  "debtor_phone": "3001234567",
  "installments_total": 12,
  "installments_paid": 3,
  "paid_installments_count": 3,
  "installments_pending": 9,
  "total_value": 300000,
  "debt_total": 300000,
  "total_paid": 90000,
  "current_debt": 210000,
  "interest_percent": 0,
  "installment_value": 25000,
  "notes": "Crédito desde n8n",
  "status": "active",
  "status_label": "Crédito activo",
  "last_payment_amount": 25000,
  "last_payment_at": "2026-03-25",
  "last_payment_type": "cuota"
}
```

Tipos disponibles:
- `product_credit`
- `cash_loan`

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/credits?q=juan"
```

### `GET /api/credits/{id}`

Devuelve el detalle completo de un crédito visible dentro del tenant autenticado.

Reglas:
- respeta tenant scope y visibilidad del crédito
- sirve tanto para `product_credit` como para `cash_loan`
- devuelve métricas derivadas actualizadas (`debt_total`, `total_paid`, `current_debt`)

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/credits/10"
```

Respuesta:

```json
{
  "ok": true,
  "credit": {
    "id": 10,
    "created_at": "2026-03-25",
    "kind": "cash_loan",
    "kind_label": "Préstamo",
    "product_id": "",
    "product": "Préstamo de dinero",
    "quantity": 1,
    "customer_id": 4,
    "customer_name": "Juan Perez",
    "customer_phone": "3001234567",
    "customer_document_type": "CC",
    "customer_document_number": "123456789",
    "customer_address": "Calle 10 # 20-30",
    "customer_city": "Bogota",
    "customer_notes": "Cliente frecuente",
    "debtor_name": "Juan Perez",
    "debtor_document_type": "CC",
    "debtor_document_number": "123456789",
    "debtor_phone": "3001234567",
    "installments_total": 6,
    "installments_paid": 2,
    "paid_installments_count": 2,
    "installments_pending": 4,
    "total_value": 180000,
    "debt_total": 180000,
    "total_paid": 60000,
    "current_debt": 120000,
    "interest_percent": 0,
    "installment_value": 30000,
    "notes": "Préstamo reprogramado",
    "status": "suspended",
    "status_label": "Crédito suspendido",
    "last_payment_amount": 30000,
    "last_payment_at": "2026-03-25",
    "last_payment_type": "cuota"
  }
}
```

### `POST /api/credits`

Registra un crédito del tenant autenticado.

Valores válidos para `kind`:
- `product_credit`
- `cash_loan`

Si no envías `kind`, la API usa `product_credit`.

Payload:

```json
{
  "kind": "product_credit",
  "product_id": "P-001",
  "quantity": 1,
  "customer_name": "Juan Perez",
  "customer_phone": "3001234567",
  "customer_document_type": "CC",
  "customer_document_number": "123456789",
  "customer_address": "Calle 10 # 20-30",
  "customer_city": "Bogota",
  "customer_notes": "Cliente frecuente",
  "debtor_name": "Juan Perez",
  "debtor_document_type": "CC",
  "debtor_document_number": "123456789",
  "debtor_phone": "3001234567",
  "installments_total": 12,
  "total_value": 300000,
  "interest_percent": 0,
  "notes": "Crédito desde n8n"
}
```

Notas:
- `product_credit` mantiene el flujo actual basado en producto.
- `cash_loan` no requiere `product_id`, no toca inventario y no genera movimientos físicos.
- si quieres un contrato más directo para agentes/n8n orientado a préstamo, usa `POST /api/agent/credits`
- `customer_*` es el contrato recomendado hacia adelante.
- `debtor_*` sigue aceptándose por compatibilidad y se usa como alias del cliente.
- `customer_city` es obligatoria para créditos nuevos.
- `customer_id` también puede enviarse para reutilizar un cliente existente del mismo tenant.
- `quantity` para `cash_loan` queda en `1` por compatibilidad y no representa unidades físicas.

Valores válidos para `debtor_document_type`:
- `CC`
- `C Extranjeria`
- `Pasaporte`

Ejemplo `cash_loan`:

```json
{
  "kind": "cash_loan",
  "customer_name": "Juan Perez",
  "customer_phone": "3001234567",
  "customer_document_type": "CC",
  "customer_document_number": "123456789",
  "customer_city": "Bogota",
  "customer_address": "Calle 10 # 20-30",
  "installments_total": 6,
  "total_value": 600000,
  "interest_percent": 0,
  "notes": "Préstamo de dinero"
}
```

Respuesta:

```json
{
  "ok": true,
  "credit_sale_id": 10,
  "customer_id": 4,
  "kind": "product_credit",
  "kind_label": "Crédito",
  "product_id": "P-001",
  "product_name": "Crema corporal",
  "quantity": 1,
  "installment_value": 25000,
  "debt_total": 300000,
  "total_paid": 0,
  "current_debt": 300000,
  "message": "Venta a crédito registrada correctamente."
}
```

Respuesta ejemplo para `cash_loan`:

```json
{
  "ok": true,
  "credit_sale_id": 10,
  "customer_id": 4,
  "kind": "cash_loan",
  "kind_label": "Préstamo",
  "product_id": "",
  "product_name": "Préstamo de dinero",
  "quantity": 1,
  "installment_value": 100000,
  "debt_total": 600000,
  "total_paid": 0,
  "current_debt": 600000,
  "message": "Préstamo registrado correctamente."
}
```

### `PUT /api/credits/{id}` o `PATCH /api/credits/{id}`

Edita un crédito existente reutilizando la misma lógica compartida que usa la app.

Permisos:
- solo `admin` y `platform_admin`

Campos editables:
- `installments_total`
- `installments_paid`
- `installment_value`
- `notes`
- `status`

Valores válidos para `status`:
- `active`
- `suspended`
- `cancelled`
- `completed`

Reglas importantes:
- no cambia `product_id`, `kind`, `tenant_id`, `created_by` ni el historial de pagos ya registrados
- `installments_total` debe ser mayor a `0`
- `installment_value` debe ser mayor a `0`
- `installments_paid` no puede quedar por debajo de las cuotas ya registradas en `credit_installments`
- `installments_paid` no puede superar `installments_total`
- no puedes marcar `completed` si todavía existe `current_debt`
- `total_paid` y `current_debt` se recalculan sin borrar pagos previos

Ejemplo:

```bash
curl -X PATCH "https://login.stockiapp.co/api/credits/10" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "installments_total": 6,
    "installments_paid": 2,
    "installment_value": 30000,
    "notes": "Crédito reprogramado por acuerdo con el cliente",
    "status": "suspended"
  }'
```

Respuesta:

```json
{
  "ok": true,
  "credit": {
    "id": 10,
    "kind": "cash_loan",
    "kind_label": "Préstamo",
    "installments_total": 6,
    "installments_paid": 2,
    "paid_installments_count": 2,
    "installments_pending": 4,
    "debt_total": 180000,
    "total_paid": 60000,
    "current_debt": 120000,
    "installment_value": 30000,
    "notes": "Crédito reprogramado por acuerdo con el cliente",
    "status": "suspended",
    "status_label": "Crédito suspendido"
  },
  "message": "Crédito actualizado correctamente."
}
```

Compatibilidad:
- funciona para `product_credit` y `cash_loan`
- en `product_credit` mantiene intacta la relación con el producto original
- en `cash_loan` no toca producto ni inventario

### `GET /api/credits/{id}/history`

Devuelve el historial compacto de ediciones del crédito usando `audit_events`.

Uso recomendado:
- soporte operativo
- trazabilidad de cambios de crédito
- inspección rápida desde la app o desde automatizaciones

Reglas:
- respeta tenant scope y visibilidad del crédito
- devuelve solo eventos de edición reales (`credit_sale_updated`)
- si no hubo cambios efectivos, la actualización no genera entrada nueva en este historial
- `limit` es opcional, default `20`, máximo `200`

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/credits/10/history?limit=10"
```

Respuesta:

```json
{
  "ok": true,
  "credit_sale_id": 10,
  "count": 1,
  "items": [
    {
      "id": 81,
      "event_type": "credit_sale_updated",
      "event_label": "Crédito editado",
      "source": "api",
      "created_at": "2026-03-25",
      "created_by": "tenant2.admin",
      "change_count": 5,
      "changes": [
        {
          "field": "installments_total",
          "label": "Cuotas totales",
          "before": 4,
          "after": 6
        },
        {
          "field": "status",
          "label": "Estado",
          "before": "active",
          "after": "suspended"
        }
      ],
      "impact": {
        "debt_total_before": 120000,
        "debt_total_after": 180000,
        "total_paid_before": 30000,
        "total_paid_after": 30000,
        "current_debt_before": 90000,
        "current_debt_after": 150000,
        "status_before": "active",
        "status_after": "suspended",
        "status_label_before": "Crédito activo",
        "status_label_after": "Crédito suspendido",
        "installments_due_after": 5
      }
    }
  ]
}
```

### `GET /api/credits/edited`

Devuelve el reporte global de créditos editados dentro del tenant autenticado.

Uso recomendado:
- soporte operativo
- seguimiento comercial de cambios sobre créditos
- revisión rápida de impacto en deuda y estado

Reglas:
- solo `admin` y `platform_admin` por sesión web
- tenant-scoped por la sesión autenticada
- reutiliza la trazabilidad ya registrada en `audit_events`
- no requiere `tenant_id`
- las API keys operativas responden `403`

Filtros disponibles:
- `date_from`
- `date_to`
- `username`
- `status`
- `kind`
- `customer`
- `credit_sale_id`
- `limit` opcional, default `100`, máximo `500`

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/credits/edited?date_from=2026-03-01&date_to=2026-03-31&status=suspended&kind=product_credit&customer=juan"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "audit_id": 81,
      "credit_sale_id": 10,
      "created_at": "2026-03-25",
      "source": "api",
      "username": "tenant2.admin",
      "tenant_id": 2,
      "tenant_slug": "tenant-dos",
      "tenant_name": "Tenant Dos",
      "kind": "product_credit",
      "kind_label": "Crédito",
      "product_id": "P-001",
      "product_name": "Crema corporal",
      "customer_id": 4,
      "customer_name": "Juan Perez",
      "customer_document": "123456789",
      "customer_phone": "3001234567",
      "status": "suspended",
      "status_label": "Crédito suspendido",
      "status_before": "active",
      "status_after": "suspended",
      "status_label_before": "Crédito activo",
      "status_label_after": "Crédito suspendido",
      "changed_fields": [
        "installments_total",
        "installments_paid",
        "installment_value",
        "notes",
        "status"
      ],
      "changed_fields_text": "installments_total, installments_paid, installment_value, notes, status",
      "change_count": 5,
      "changes": [
        {
          "field": "installments_total",
          "label": "Cuotas totales",
          "before": 4,
          "after": 6,
          "before_text": "4",
          "after_text": "6"
        }
      ],
      "debt_total_before": 120000,
      "debt_total_after": 180000,
      "total_paid_before": 30000,
      "total_paid_after": 30000,
      "current_debt_before": 90000,
      "current_debt_after": 150000,
      "current_debt_delta": 60000,
      "installments_due_now": 5
    }
  ]
}
```

### `GET /api/credits/installments`

Devuelve pagos individuales (`cuotas` y `abonos`) para reportes de recaudación.

Reglas:
- tenant-scoped por sesión o Bearer token
- respeta la misma visibilidad de créditos y ownership que `GET /api/credits`
- `from` usa formato `YYYY-MM-DD` como límite inferior sobre `credit_installments.created_at`
- `to` usa formato `YYYY-MM-DD` como límite superior sobre `credit_installments.created_at`
- `q` busca por nombre del cliente o nombre del producto
- para créditos de producto, la persistencia interna sigue usando `credit_installments.product_id` y `credit_sales.product_id` con `sku`
- la salida humana devuelve solo datos compactos para recaudo; no expone `sku`
- para `cash_loan`, `product_name` se devuelve como `Préstamo de dinero`

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/credits/installments?from=2026-03-01&to=2026-03-31&q=juan"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 1,
      "credit_sale_id": 10,
      "customer_name": "Juan Perez",
      "product_name": "iPhone 12",
      "amount_paid": 25000,
      "payment_type": "cuota",
      "created_at": "2026-03-29"
    }
  ]
}
```

### `POST /api/credits/installments`

Ver la sección [Pagos, Cuotas Y Abonos](#pagos-cuotas-y-abonos) para el contrato completo de cuotas y abonos.

## Pagos, Cuotas Y Abonos

### `POST /api/credits/installments`

Este es el endpoint oficial para registrar:
- `cuota`
- `abono`

Reglas:
- `credit_sale_id` debe ser numérico
- `payment_type` es opcional
- si no envías `payment_type`, la API usa `cuota`
- el mismo endpoint sirve para créditos con producto y para `cash_loan`

Ejemplo `abono`:

```json
{
  "credit_sale_id": 10,
  "amount_paid": 5000,
  "payment_type": "abono"
}
```

Ejemplo `curl`:

```bash
curl -X POST "https://login.stockiapp.co/api/credits/installments" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "credit_sale_id": 10,
    "amount_paid": 5000,
    "payment_type": "abono"
}'
```

Respuesta:

```json
{
  "ok": true,
  "credit_installment_id": 42,
  "credit_sale_id": 10,
  "product_name": "Puma Dama 40605802",
  "amount_paid": 5000,
  "paid_installments": 2,
  "total_installments": 6,
  "pending_installments": 4,
  "current_debt": 70000,
  "paid_at": "2026-07-26T15:30:00-05:00",
  "receipt_url": "/creditos/comprobante-pago?installment_id=42",
  "thermal_ticket_url": "/creditos/ticket-pago?installment_id=42"
}
```

`receipt_url` y `thermal_ticket_url` corresponden exclusivamente al pago recién registrado. Los datos del comprobante se guardan como snapshot y no cambian si luego se registra otro pago o se edita el crédito.

### `GET /api/credits/installments/{id}/receipt`

Recupera el snapshot inmutable y las URLs del comprobante de un pago específico.

Reglas:
- `{id}` es `credit_installment_id`, no `credit_sale_id`
- respeta tenant y ownership del crédito
- no recibe `tenant_id`
- pagos históricos sin snapshot responden `404`; no se recalculan con el estado actual del crédito

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/credits/installments/42/receipt"
```

Respuesta:

```json
{
  "ok": true,
  "item": {
    "credit_installment_id": 42,
    "credit_sale_id": 10,
    "customer_name": "Laura Perez",
    "product_name": "Puma Dama 40605802",
    "payment_type": "abono",
    "amount_paid": 5000,
    "current_debt": 70000,
    "paid_installments": 2,
    "total_installments": 6,
    "pending_installments": 4,
    "paid_at": "2026-07-26T15:30:00-05:00",
    "receipt_url": "/creditos/comprobante-pago?installment_id=42",
    "thermal_ticket_url": "/creditos/ticket-pago?installment_id=42"
  }
}
```

## Endpoints Para Agente

Estos endpoints están pensados para automatización y respuestas más compactas.

Reglas:
- se autentican igual con `Authorization: Bearer <token>`
- no necesitan `tenant_id`
- respetan ownership y tenant activo

### `GET /api/agent/business`

Devuelve el contexto mínimo que un workflow necesita para operar.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  https://login.stockiapp.co/api/agent/business
```

Respuesta:

```json
{
  "ok": true,
  "item": {
    "business_name": "Tenant Dos Brand",
    "contact_phone": "+57 300 123 4567",
    "contact_email": "ventas@tenantdos.co",
    "social_media": "Instagram @tenantdos",
    "primary_color": "#112233",
    "currency": "USD",
    "date_format": "2006-01-02",
    "tenant": {
      "id": 2,
      "slug": "tenant-dos",
      "name": "Tenant Dos"
    },
    "auth_mode": "api_key",
    "integration_name": "tenant-dos-inicial"
  }
}
```

Uso recomendado:
- primer paso de cada workflow
- validación del tenant resuelto
- inicio de trazabilidad del agente

### `GET /api/agent/products/search?q=`

Busca productos visibles por `id`, nombre, línea o locación.

El filtro `q` es opcional y se compara por tokens: todos los términos deben aparecer en alguno de esos campos o en el nombre del deudor, sin importar el orden ni palabras intermedias. La comparación no distingue mayúsculas ni tildes. Por ejemplo, `q=Puma 40605802` encuentra `Puma Dama 40605802` y `q=camion` encuentra `Camión`.

### `GET /api/agent/customers/search?q=`

Búsqueda compacta de clientes para agente/n8n.

Uso recomendado:
- buscar y reutilizar `customer_id` antes de crear `cash_loan` o `product_credit`
- evitar crear clientes duplicados cuando ya existe uno equivalente en el tenant

Busca por:
- nombre
- teléfono
- tipo/número de documento
- ciudad

Parámetros:
- `q`
- `limit` opcional, default `20`, máximo `100`

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/agent/customers/search?q=juan"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 4,
      "name": "Juan Perez",
      "phone": "3001234567",
      "document_type": "CC",
      "document_number": "123456789",
      "city": "Bogota",
      "credits_count": 2,
      "debt_total": 600000,
      "total_paid": 150000,
      "current_debt": 450000,
      "active_credits": 2,
      "last_credit_at": "2026-03-25"
    }
  ]
}
```

Notas:
- la respuesta es compacta a propósito
- si necesitas ficha completa usa `GET /api/customers/{id}`
- el tenant se resuelve por Bearer token o API key; no envíes `tenant_id`

### `GET /api/agent/products/price?id=`

Consulta rápida de precio de venta, locación y valor de retoma por producto.

Notas:
- `id` debe ser el identificador visible del producto
- no acepta `sku` interno como selector implícito

### `GET /api/agent/inventory?q=`

Consulta rápida de disponibilidad con formato compacto para automatización.

También permite encontrar productos por locación. El filtro `q` es opcional y usa la misma búsqueda por tokens, sin distinguir mayúsculas ni tildes, que `GET /api/agent/products/search`.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/agent/inventory?q=Puma%2040605802"
```

### `GET /api/agent/product-loans?customer_id=`

Lista préstamos físicos activos del tenant actual.

Notas:
- devuelve una fila por unidad prestada activa
- `customer_id` es opcional y permite filtrar por cliente
- `fecha_inicio` se devuelve en RFC3339 ya normalizado a `America/Bogota`
- `estado` puede ser `active`, `returned`, `paid` o `cancelled`
- no recibe `tenant_id`; el tenant se resuelve por Bearer token o API key
- registra auditoría con `source = "api"`

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/agent/product-loans?customer_id=42"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "loan_id": 12,
      "customer_name": "Laura Prestamo",
      "customer_phone": "3007770001",
      "product_sku": "SKU-AGENT-LOAN-1",
      "product_name": "Camisa prestada",
      "unit_serial": "UNIT-LOAN-001",
      "fecha_inicio": "2026-04-10T11:16:00-05:00",
      "estado": "active"
    }
  ]
}
```

### `POST /api/agent/credits`

Wrapper pensado para agente/n8n.

Reglas:
- usa el mismo dominio y validaciones de `POST /api/credits`
- si no envías `kind`, el endpoint usa `cash_loan`
- si envías `kind = product_credit`, reutiliza el flujo actual de crédito con producto
- no recibe `tenant_id`; el tenant se resuelve por Bearer token o API key

Payload mínimo recomendado para `cash_loan`:

```json
{
  "customer_name": "Juan Perez",
  "customer_phone": "3001234567",
  "customer_document_type": "CC",
  "customer_document_number": "123456789",
  "customer_city": "Bogota",
  "installments_total": 6,
  "total_value": 600000,
  "interest_percent": 0,
  "notes": "Prestamo de dinero desde agente"
}
```

Ejemplo `curl` para `cash_loan`:

```bash
curl -X POST "https://login.stockiapp.co/api/agent/credits" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "Juan Perez",
    "customer_phone": "3001234567",
    "customer_document_type": "CC",
    "customer_document_number": "123456789",
    "customer_city": "Bogota",
    "installments_total": 6,
    "total_value": 600000,
    "interest_percent": 0,
    "notes": "Prestamo de dinero desde agente"
  }'
```

Ejemplo `curl` para `product_credit` explícito:

```bash
curl -X POST "https://login.stockiapp.co/api/agent/credits" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": "product_credit",
    "product_id": "P-001",
    "quantity": 1,
    "customer_name": "Juan Perez",
    "customer_phone": "3001234567",
    "customer_document_type": "CC",
    "customer_document_number": "123456789",
    "customer_city": "Bogota",
    "installments_total": 6,
    "total_value": 300000,
    "interest_percent": 0,
    "notes": "Credito con producto desde agente"
  }'
```

Respuesta:

```json
{
  "ok": true,
  "credit_sale_id": 12,
  "customer_id": 4,
  "kind": "cash_loan",
  "kind_label": "Préstamo",
  "product_id": "",
  "product_name": "Préstamo de dinero",
  "quantity": 1,
  "installment_value": 100000,
  "debt_total": 600000,
  "total_paid": 0,
  "current_debt": 600000,
  "message": "Préstamo registrado correctamente."
}
```

## Compatibilidad / Legado

### `GET /api/productos/precio`

Endpoint legado para consultar el precio de venta de un producto.

Acepta:
- `?id=P-001`
- `?sku=SKU-000001` solo como compatibilidad explícita

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/productos/precio?id=P-001"
```

Nota:
- se conserva por compatibilidad
- `id` visible es el selector recomendado
- si usas `sku`, debe ser el `sku` interno real; ya no se resuelve como alias implícito del `id` visible
- para nuevas integraciones, usa preferiblemente `id` visible y los endpoints modernos de `products` o `agent`

## Códigos HTTP Usados

- `200 OK`: lectura exitosa
- `201 Created`: escritura exitosa
- `400 Bad Request`: datos inválidos
- `401 Unauthorized`: autenticación requerida, API key inválida o tenant inactivo
- `403 Forbidden`: sin permisos o movimiento deshabilitado
- `404 Not Found`: recurso no encontrado cuando aplica
- `405 Method Not Allowed`: método HTTP no permitido
- `500 Internal Server Error`: error interno

## Reglas De Visibilidad Y Permisos

### Productos

- Admin ve todos los productos del tenant.
- Usuario normal ve productos públicos y propios.
- `owner_user_id = null` significa producto público.
- `owner_user_id != null` significa producto asignado a un usuario concreto.

### Ventas, cambios, retomas y crédito

- Solo pueden operar productos visibles para el usuario autenticado.
- Si un producto no es visible, la API responde error.
- Si `venta`, `cambio`, `retoma` o `credito` están deshabilitados en configuración, la API responde `403`.

## Auditoría

Las escrituras por API generan eventos en `audit_events` con:
- `source = "api"`
- `integration_name` en metadata cuando la autenticación entra por API key

Eventos relevantes:
- `product_created`
- `product_assigned`
- `product_updated`
- `inventory_adjusted`
- `retoma_registered`
- `sale_registered`
- `credit_sale_created`
- `credit_installment_added`
- `change_registered`

## Recomendaciones Para Integraciones

1. Usa una API key por negocio.
2. Mantén un workflow por tenant.
3. Empieza siempre con `GET /api/health`.
4. Inicializa contexto con `GET /api/agent/business`.
5. Usa `GET /api/agent/customers/search` para reutilizar `customer_id` antes de crear créditos o préstamos.
6. Usa `GET /api/agent/products/search` y `GET /api/agent/inventory` para evitar contratos pesados.
7. Usa `GET /api/settings/lines` y `GET /api/settings/owners` antes de crear o asignar productos.
8. No mandes `tenant_id` manual en payloads.

Si más adelante quieres, este documento también puede servir como base para una colección Postman o una especificación OpenAPI.
