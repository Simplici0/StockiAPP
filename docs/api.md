# API interna de Stocki App

Esta es la referencia oficial de la API multi-tenant de Stocki App para automatizaciones, agentes y workflows de `n8n`.

La regla operativa es simple:
- cada negocio usa su propia API key
- el backend resuelve el `tenant` automáticamente
- no se envía `tenant_id` manual ni en auth ni en payloads
- si cambia solo el `APIKEY`, cambia solo el negocio al que apunta el workflow

## Modelo De Acceso

Stocki App usa una sola app, una sola base de datos y aislamiento lógico por `tenant_id`.

Para agentes, el canal oficial es:

```http
Authorization: Bearer <APIKEY>
```

Reglas importantes:
- `Bearer` tiene prioridad sobre la sesión web cuando ambos llegan a `/api/*`
- si no llega `Bearer`, `/api/*` todavía puede usar sesión por compatibilidad
- para automatizaciones nuevas, el contrato oficial es `Bearer`
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
Si la request viene de un admin, puede incluir inactivas con `?include_inactive=true`.

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

Solo administrador del tenant.

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

### `GET /api/products/search?q=`

Busca productos visibles por `id`, nombre, línea o deudor.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/products/search?q=crema"
```

### `POST /api/products`

Crea un producto y sus unidades iniciales.

Solo admin.

Payload:

```json
{
  "name": "Producto API",
  "line": "Farmacia",
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
- `quantity` debe ser mayor a `0`
- `owner_user_id` es opcional
- `retoma_enabled=true` exige `retoma_price` válido
- `aplica_caducidad=true` exige `fecha_caducidad` en `YYYY-MM-DD`

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

Solo personal autorizado.

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
  "notes": "Retoma desde n8n"
}
```

Respuesta:

```json
{
  "ok": true,
  "retoma_id": 10,
  "product_id": "P-001",
  "product_name": "Crema corporal",
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
  "message": "Venta registrada correctamente."
}
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

## Créditos

### `GET /api/credits`

Lista créditos visibles para el usuario autenticado.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/credits?q=juan"
```

### `POST /api/credits`

Registra una venta a crédito.

Payload:

```json
{
  "product_id": "P-001",
  "quantity": 1,
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

Valores válidos para `debtor_document_type`:
- `CC`
- `C Extranjeria`
- `Pasaporte`

Respuesta:

```json
{
  "ok": true,
  "credit_sale_id": 10,
  "product_id": "P-001",
  "product_name": "Crema corporal",
  "quantity": 1,
  "installment_value": 25000,
  "message": "Venta a crédito registrada correctamente."
}
```

### `POST /api/credits/installments`

Registra una cuota de un crédito existente.

Payload:

```json
{
  "credit_sale_id": 10,
  "amount_paid": 25000
}
```

Respuesta:

```json
{
  "ok": true,
  "credit_sale_id": 10,
  "product_id": "P-001",
  "amount_paid": 25000,
  "installment_number": 2,
  "message": "Cuota 2 registrada correctamente."
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

Busca productos visibles por `id`, nombre o línea.

### `GET /api/agent/products/price?id=`

Consulta rápida de precio de venta y valor de retoma por producto.

### `GET /api/agent/inventory?q=`

Consulta rápida de disponibilidad con formato compacto para automatización.

## Compatibilidad / Legado

### `GET /api/productos/precio`

Endpoint legado para consultar el precio de venta de un producto.

Acepta:
- `?id=P-001`
- `?sku=P-001`

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://login.stockiapp.co/api/productos/precio?id=P-001"
```

Nota:
- se conserva por compatibilidad
- para nuevas integraciones, usa preferiblemente los endpoints modernos de `products` o `agent`

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
5. Usa `GET /api/agent/products/search` y `GET /api/agent/inventory` para evitar contratos pesados.
6. Usa `GET /api/settings/lines` y `GET /api/settings/owners` antes de crear o asignar productos.
7. No mandes `tenant_id` manual en payloads.

Si más adelante quieres, este documento también puede servir como base para una colección Postman o una especificación OpenAPI.
