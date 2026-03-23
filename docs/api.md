# API interna de Stocki App

Esta API permite consultar y operar Stocki App sin acceder directamente a la base de datos.

Estado actual:
- Base URL producción: `https://login.stockiapp.co`
- Formato: `application/json`
- Autenticación soportada:
  - `Authorization: Bearer <token>`

## Autenticación

Los endpoints `/api/*` aceptan autenticación vía header `Authorization: Bearer <token>`.

Si no envías un Bearer token válido, la API responde:

```json
{
  "ok": false,
  "error": "Autenticación requerida para la API."
}
```

con código HTTP `401 Unauthorized`.

### API key

Las API keys se crean desde `Configuración > API keys`.

Reglas:
- el token solo se muestra al momento de creación
- en base de datos solo se guarda `token_hash`
- si la key está inactiva, deja de autenticar inmediatamente
- por ahora una API key válida opera con contexto admin controlado

Ejemplo de uso:

```bash
curl -H "Authorization: Bearer TU_TOKEN" https://login.stockiapp.co/api/health
```

### Uso en n8n / producción

Para n8n en producción usa la URL oficial:

- `https://login.stockiapp.co`

Headers recomendados en todos los nodos `HttpRequest`:

```http
Authorization: Bearer TU_TOKEN
Accept: application/json
```

Si el nodo hace `POST` con JSON, agrega también:

```http
Content-Type: application/json
```

Ejemplos rápidos con la URL oficial:

Healthcheck:

```bash
curl -X GET "https://login.stockiapp.co/api/health" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json"
```

Consultar inventario:

```bash
curl -X GET "https://login.stockiapp.co/api/inventory" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json"
```

Consultar retomas:

```bash
curl -X GET "https://login.stockiapp.co/api/retomas" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json"
```

Ajustar inventario:

```bash
curl -X POST "https://login.stockiapp.co/api/inventory/adjust" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "P-001",
    "target_quantity": 10,
    "notes": "Ajuste desde n8n"
  }'
```

Registrar retoma:

```bash
curl -X POST "https://login.stockiapp.co/api/retomas" \
  -H "Authorization: Bearer TU_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "P-001",
    "quantity": 1,
    "value_received": 12000,
    "received_state": "Usado",
    "publish_to_stock": true,
    "final_sale_price": 25000,
    "notes": "Retoma desde n8n"
  }'
```

## Reglas generales

- Todos los endpoints `/api/*` responden JSON.
- Las escrituras reutilizan la lógica actual del sistema.
- Las reglas de stock, ownership y tipos de movimiento siguen aplicando.
- Las acciones hechas por API registran auditoría con `source = "api"`.
- Si la llamada entra por API key, la auditoría puede incluir `integration_name`.
- Si un producto tiene `owner_user_id`, solo lo ve su usuario asignado y admin.
- Los productos sin `owner_user_id` son públicos para usuarios autorizados.

## Formato de respuesta

Respuesta exitosa típica:

```json
{
  "ok": true
}
```

Respuesta con error típica:

```json
{
  "ok": false,
  "error": "Mensaje de error",
  "fields": {
    "campo": "Detalle opcional"
  }
}
```

## API General

## Salud

### GET /api/health

Healthcheck JSON de la API. Requiere Bearer token válido.

Ejemplo con API key:

```bash
curl -H "Authorization: Bearer TU_TOKEN" https://login.stockiapp.co/api/health
```

Respuesta:

```json
{
  "ok": true,
  "service": "stocki-app"
}
```

## Productos e Inventario

### GET /api/products

Lista productos visibles para el usuario autenticado.

Uso recomendado:
- sincronizar catálogo visible
- validar `owner_user_id`
- obtener `sale_price` y configuración de `retoma`

Respuesta:

```json
{
  "ok": true,
  "count": 2,
  "items": [
    {
      "id": "P-001",
      "name": "Crema corporal",
      "line": "Farmacia",
      "fecha_ingreso": "2026-03-12",
      "sale_price": 25000,
      "retoma_enabled": true,
      "retoma_price": 12000,
      "owner_user_id": 2
    }
  ]
}
```

```bash
curl -H "Authorization: Bearer TU_TOKEN" https://login.stockiapp.co/api/products
```

### GET /api/products/search?q=

Busca productos visibles por `id`, nombre o línea.

Uso recomendado:
- búsquedas desde agentes
- validación previa antes de vender, cambiar o registrar retoma

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/products/search?q=crema"
```

### POST /api/products

Crea un producto y sus unidades iniciales. Solo admin.

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

Notas:
- `owner_user_id` es opcional.
- `line` debe existir y estar activa.
- `quantity` debe ser mayor a `0`.
- `retoma_enabled` es opcional.
- Si `retoma_enabled=true`, `retoma_price` debe ser válido.
- Si `aplica_caducidad=true`, `fecha_caducidad` debe ir en formato `YYYY-MM-DD`.

Ejemplo:

```bash
curl -X POST https://login.stockiapp.co/api/products \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TU_TOKEN" \
  -d '{
    "name": "Producto API",
    "line": "Farmacia",
    "owner_user_id": 2,
    "quantity": 5,
    "sale_price": 25000,
    "retoma_enabled": true,
    "retoma_price": 12000,
    "aplica_caducidad": false
  }'
```

Respuesta:

```json
{
  "ok": true,
  "id": "P-010",
  "message": "Producto creado correctamente."
}
```

### GET /api/inventory

Devuelve resumen de inventario por producto visible.

Uso recomendado:
- consulta operativa de stock
- monitoreo desde n8n
- validación previa antes de automatizar ajustes o ventas

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
      "available": 8,
      "reserved": 1,
      "swapped": 0,
      "damaged": 0,
      "sale_price": 25000,
      "retoma_enabled": true,
      "retoma_price": 12000,
      "owner_user_id": null
    }
  ]
}
```

```bash
curl -H "Authorization: Bearer TU_TOKEN" https://login.stockiapp.co/api/inventory
```

### POST /api/inventory/adjust

Ajusta el inventario de un producto y, opcionalmente, actualiza nombre, precio de venta y configuración de retoma.

Respeta:
- ownership
- stock disponible al reducir
- auditoría y movimientos del sistema

Payload:

```json
{
  "product_id": "P-001",
  "target_quantity": 10,
  "sale_price": 26000,
  "retoma_enabled": true,
  "retoma_price": 12000,
  "notes": "Ajuste desde API"
}
```

Notas:
- `target_quantity` es opcional. Si no va, puedes actualizar solo datos del producto.
- `sale_price` es opcional.
- `name` es opcional.
- Si envías `retoma_price`, también debes enviar `retoma_enabled`.
- Si `retoma_enabled=true`, `retoma_price` es obligatorio y no puede superar `sale_price`.

Ejemplo:

```bash
curl -X POST https://login.stockiapp.co/api/inventory/adjust \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TU_TOKEN" \
  -d '{
    "product_id": "P-001",
    "target_quantity": 10,
    "sale_price": 26000,
    "retoma_enabled": true,
    "retoma_price": 12000,
    "notes": "Ajuste desde API"
  }'
```

Respuesta:

```json
{
  "ok": true,
  "product_id": "P-001",
  "previous_quantity": 8,
  "current_quantity": 10,
  "delta": 2,
  "message": "Stock, precio de venta y retoma actualizados correctamente."
}
```

## Retomas

### GET /api/retomas

Lista retomas visibles para el usuario autenticado.

Puedes filtrar con `?q=` por producto, estado recibido o notas.

Uso recomendado:
- seguimiento de retomas registradas
- conciliación operativa
- consulta desde agentes o flujos n8n

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/retomas?q=usado"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 7,
      "fecha": "2026-03-12",
      "product_id": "P-001",
      "product_name": "Crema corporal",
      "quantity": 1,
      "value_received": 12000,
      "received_state": "Usado",
      "published_to_stock": true,
      "final_sale_price": 25000,
      "notes": "Retoma desde API"
    }
  ]
}
```

### POST /api/retomas

Registra una retoma. Respeta:
- ownership
- tipo de movimiento `retoma` habilitado
- configuración `retoma_enabled` del producto

Payload:

```json
{
  "product_id": "P-001",
  "quantity": 1,
  "value_received": 12000,
  "received_state": "Usado",
  "publish_to_stock": true,
  "final_sale_price": 25000,
  "notes": "Retoma desde API"
}
```

Notas:
- `received_state` debe ser uno de: `Nuevo`, `Usado`, `Dañado`, `Para repuestos`, `Otro`.
- `final_sale_price` es opcional y solo se aplica cuando `publish_to_stock=true`.
- Si `publish_to_stock=true`, se crean unidades disponibles y se registra movimiento `retoma_stock`.
- El evento de auditoría asociado es `retoma_registered`.

Ejemplo:

```bash
curl -X POST https://login.stockiapp.co/api/retomas \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TU_TOKEN" \
  -d '{
    "product_id": "P-001",
    "quantity": 1,
    "value_received": 12000,
    "received_state": "Usado",
    "publish_to_stock": true,
    "final_sale_price": 25000,
    "notes": "Retoma desde API"
  }'
```

Respuesta:

```json
{
  "ok": true,
  "retoma_id": 7,
  "product_id": "P-001",
  "product_name": "Crema corporal",
  "quantity": 1,
  "value_received": 12000,
  "received_state": "Usado",
  "published_to_stock": true,
  "units_created": 1,
  "message": "Retoma registrada y publicada a stock correctamente."
}
```

## Ventas

La API expone dos vistas para ventas:

- `GET /api/sales/recent`: vista rápida, compacta y compatible con el formato histórico del proyecto
- `GET /api/sales`: listado más completo para integraciones como n8n, con filtros y campos extendidos

La escritura sigue entrando por:

- `POST /api/sales`

### GET /api/sales/recent

Devuelve las ventas recientes visibles para el usuario autenticado.

Uso recomendado:
- consultas rápidas
- compatibilidad con integraciones ya existentes
- consumo liviano sin filtros

Diferencias frente a `GET /api/sales`:
- devuelve solo las ventas más recientes
- conserva nombres históricos del modelo de salida como `producto_id`, `producto`, `cantidad`, `precio_final`, `metodo_pago`
- no soporta filtros `q`, `from`, `to`

```bash
curl -H "Authorization: Bearer TU_TOKEN" https://login.stockiapp.co/api/sales/recent
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 42,
      "fecha": "2026-03-12",
      "producto_id": "P-001",
      "producto": "P-001",
      "cantidad": 1,
      "precio_final": 25000,
      "metodo_pago": "Efectivo",
      "total": 25000
    }
  ]
}
```

### GET /api/sales

Lista ventas visibles para el usuario autenticado.

Uso recomendado:
- integraciones con n8n
- agentes
- búsquedas y filtros operativos
- listados con contrato más explícito y estable para automatización

Formato de salida:
- usa nombres orientados a integración como `product_id`, `product_name`, `sale_price`, `channel`, `sold_by`
- incluye también `payment_method` para no perder el dato canónico del sistema

Filtros opcionales:
- `q` busca por `product_id`, nombre del producto, `channel`, `sold_by`, `notes` y `payment_method`
- `from` en formato `YYYY-MM-DD`
- `to` en formato `YYYY-MM-DD`

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/sales"
```

Ejemplo con filtros:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/sales?q=tienda&from=2026-03-01&to=2026-03-31"
```

Respuesta:

```json
{
  "ok": true,
  "count": 2,
  "items": [
    {
      "id": 15,
      "fecha": "2026-03-22",
      "product_id": "P-001",
      "product_name": "Nombre del producto",
      "quantity": 1,
      "sale_price": 25000,
      "channel": "Tienda",
      "sold_by": "Mauro",
      "notes": "",
      "payment_method": "Efectivo"
    }
  ]
}
```

### POST /api/sales

Registra una venta. Respeta:
- stock disponible
- ownership
- métodos de pago activos
- tipo de movimiento `venta` habilitado

Compatibilidad:
- sigue aceptando `payment_method`
- sigue aceptando `unit_price`
- sigue aceptando `total`
- añade soporte para `sale_price`, `channel` y `sold_by`

Payload:

```json
{
  "product_id": "P-001",
  "quantity": 1,
  "sale_price": 25000,
  "channel": "Tienda",
  "sold_by": "Mauro",
  "notes": "Venta desde agente"
}
```

Notas:
- `quantity` es opcional. Si no llega, el backend usa `1`.
- `sale_price` es opcional. Si no llega, el backend usa el `precio_venta` actual del producto cuando sea válido.
- `channel` es opcional.
- `sold_by` es opcional.
- `payment_method` sigue siendo compatible y se mantiene como campo canónico del medio de pago.
- También puedes enviar `unit_price` o `total`. Si `total > 0`, el backend usa `total / quantity` como precio unitario final.
- Si no envías `payment_method`, el backend usa el primer método de pago activo configurado.
- El evento de auditoría asociado es `sale_registered`.

Prioridad del precio aplicado:
1. `total / quantity` si `total > 0`
2. `sale_price` si fue enviado
3. `unit_price` si fue enviado
4. `precio_venta` actual del producto

Ejemplo:

```bash
curl -X POST https://login.stockiapp.co/api/sales \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TU_TOKEN" \
  -d '{
    "product_id": "P-001",
    "quantity": 1,
    "sale_price": 25000,
    "channel": "Tienda",
    "sold_by": "Mauro",
    "notes": "Venta desde agente"
  }'
```

Respuesta:

```json
{
  "ok": true,
  "sale_id": 15,
  "product_id": "P-001",
  "product_name": "Nombre del producto",
  "quantity": 1,
  "sale_price": 25000,
  "message": "Venta registrada correctamente."
}
```

## Cambios

### POST /api/swaps

Registra un cambio. Respeta:
- ownership
- stock disponible del producto saliente
- tipo de movimiento `cambio` habilitado

Soporta dos modos de entrada:
- `existing`: entra stock a un producto ya existente
- `new`: crea entrada para un ID nuevo

#### Modo `existing`

Payload:

```json
{
  "product_id": "P-001",
  "quantity": 1,
  "persona_del_cambio": "Cliente API",
  "notes": "Cambio desde API",
  "incoming_mode": "existing",
  "incoming_existing_id": "P-002",
  "incoming_existing_qty": 1
}
```

Ejemplo:

```bash
curl -X POST https://login.stockiapp.co/api/swaps \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TU_TOKEN" \
  -d '{
    "product_id": "P-001",
    "quantity": 1,
    "persona_del_cambio": "Cliente API",
    "notes": "Cambio desde API",
    "incoming_mode": "existing",
    "incoming_existing_id": "P-002",
    "incoming_existing_qty": 1
  }'
```

#### Modo `new`

Payload:

```json
{
  "product_id": "P-001",
  "quantity": 1,
  "persona_del_cambio": "Cliente API",
  "notes": "Cambio desde API",
  "incoming_mode": "new",
  "incoming_new_sku": "P-900",
  "incoming_new_name": "Producto nuevo por cambio",
  "incoming_new_line": "Farmacia",
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

### GET /api/credits

Devuelve los créditos visibles para el usuario autenticado.

Soporta búsqueda opcional con `q` sobre:
- `product_id`
- nombre del producto
- nombre del deudor
- tipo de documento del deudor
- número de documento del deudor
- teléfono del deudor

Ejemplos útiles para agente / n8n:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/credits"
```

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/credits?q=maria"
```

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/credits?q=1020304050"
```

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/credits?q=3001234567"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": 12,
      "created_at": "2026-03-12",
      "product_id": "P-001",
      "product": "Crema corporal",
      "quantity": 1,
      "debtor_name": "Maria Gomez",
      "debtor_document_type": "CC",
      "debtor_document_number": "1020304050",
      "debtor_phone": "3001234567",
      "installments_total": 6,
      "installments_paid": 2,
      "installments_pending": 4,
      "total_value": 250000,
      "interest_percent": 5,
      "installment_value": 43750,
      "notes": "VENTA A CREDITO | Deudor: Maria Gomez | Cuotas: 6",
      "status": "active",
      "last_payment_amount": 43750,
      "last_payment_at": "2026-03-20"
    }
  ]
}
```

### POST /api/credits

Registra una venta a crédito. Respeta:
- stock disponible
- ownership
- tipo de movimiento `credito` habilitado

Payload:

```json
{
  "product_id": "P-001",
  "quantity": 1,
  "debtor_name": "Maria Gomez",
  "debtor_document_type": "CC",
  "debtor_document_number": "1020304050",
  "debtor_phone": "3001234567",
  "installments_total": 6,
  "total_value": 250000,
  "interest_percent": 5,
  "notes": "Crédito desde API"
}
```

Notas:
- `installment_value` es opcional.
- Si no se envía, el backend calcula la cuota con `total_value`, `installments_total` e `interest_percent`.
- Si se envía, debe ser mayor a `0`.
- `debtor_document_type` es obligatorio y hoy acepta: `CC`, `C Extranjeria`, `Pasaporte`.
- `debtor_document_number` es obligatorio.
- `debtor_phone` es obligatorio.

Ejemplo:

```bash
curl -X POST https://login.stockiapp.co/api/credits \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TU_TOKEN" \
  -d '{
    "product_id": "P-001",
    "quantity": 1,
    "debtor_name": "Maria Gomez",
    "debtor_document_type": "CC",
    "debtor_document_number": "1020304050",
    "debtor_phone": "3001234567",
    "installments_total": 6,
    "total_value": 250000,
    "interest_percent": 5,
    "notes": "Crédito desde API"
  }'
```

Respuesta:

```json
{
  "ok": true,
  "credit_sale_id": 12,
  "product_id": "P-001",
  "product_name": "Crema corporal",
  "quantity": 1,
  "installment_value": 43750,
  "message": "Venta a crédito registrada correctamente."
}
```

### POST /api/credits/installments

Registra una cuota para un crédito visible.

Payload:

```json
{
  "credit_sale_id": 12,
  "amount_paid": 43750
}
```

Notas:
- `amount_paid` es opcional.
- Si no se envía, el backend usa el `installment_value` configurado en el crédito.

Ejemplo:

```bash
curl -X POST https://login.stockiapp.co/api/credits/installments \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TU_TOKEN" \
  -d '{
    "credit_sale_id": 12,
    "amount_paid": 43750
  }'
```

Respuesta:

```json
{
  "ok": true,
  "credit_sale_id": 12,
  "product_id": "P-001",
  "amount_paid": 43750,
  "installment_number": 3,
  "message": "Cuota 3 registrada correctamente."
}
```

## Configuración

### GET /api/settings/business

Devuelve la configuración general del negocio.

```bash
curl -H "Authorization: Bearer TU_TOKEN" https://login.stockiapp.co/api/settings/business
```

Respuesta:

```json
{
  "ok": true,
  "settings": {
    "business_name": "Stocki App",
    "logo_path": "",
    "primary_color": "#0ea5c9",
    "currency": "COP",
    "date_format": "2006-01-02"
  }
}
```

### GET /api/settings/lines

Devuelve las líneas de negocio disponibles para crear productos.

Por defecto devuelve solo líneas activas. Si eres admin, puedes incluir inactivas con `?include_inactive=true`.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" https://login.stockiapp.co/api/settings/lines
```

Respuesta:

```json
{
  "ok": true,
  "count": 2,
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

### GET /api/settings/owners

Devuelve usuarios asignables con su `id` para usar en `owner_user_id` al crear o editar productos.

Solo admin.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" https://login.stockiapp.co/api/settings/owners
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

## Endpoints para Agente

Estos endpoints complementan la API general con respuestas más compactas para automatización y agentes tipo n8n.

Reglas:
- usan siempre `id` como identificador del producto
- respetan ownership y visibilidad actual
- aceptan `Authorization: Bearer <token>`
- no generan auditoría en lecturas `GET`

### GET /api/agent/products/search?q=

Busca productos visibles por `id`, nombre o línea.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/agent/products/search?q=iphone"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": "IP12-001",
      "name": "iPhone 12",
      "line": "Celulares",
      "sale_price": 1800000,
      "retoma_enabled": true,
      "retoma_price": 1400000,
      "available": 3,
      "status": "available"
    }
  ]
}
```

### GET /api/agent/products/price?id=

Consulta rápida de precio de venta y valor de retoma por `id`.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/agent/products/price?id=IP12-001"
```

Respuesta:

```json
{
  "ok": true,
  "item": {
    "id": "IP12-001",
    "name": "iPhone 12",
    "sale_price": 1800000,
    "retoma_enabled": true,
    "retoma_price": 1400000
  }
}
```

### GET /api/agent/inventory?q=

Consulta rápida de disponibilidad.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/agent/inventory?q=iphone"
```

Respuesta:

```json
{
  "ok": true,
  "count": 1,
  "items": [
    {
      "id": "IP12-001",
      "name": "iPhone 12",
      "line": "Celulares",
      "sale_price": 1800000,
      "retoma_enabled": true,
      "retoma_price": 1400000,
      "available": 3,
      "status": "available"
    }
  ]
}
```

### GET /api/agent/business

Devuelve la configuración básica útil para un agente.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" https://login.stockiapp.co/api/agent/business
```

Respuesta:

```json
{
  "ok": true,
  "item": {
    "business_name": "Stocki App",
    "currency": "COP",
    "date_format": "2006-01-02"
  }
}
```

## Compatibilidad / Legado

### GET /api/productos/precio

Endpoint legado para consultar el precio de venta de un producto por ID.

Uso:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "https://login.stockiapp.co/api/productos/precio?id=P-001"
```

Nota:
- Se conserva por compatibilidad.
- Para nuevas integraciones, usa preferiblemente `/api/products` o `/api/products/search`.

## Códigos HTTP usados

- `200 OK`: lectura exitosa
- `201 Created`: escritura exitosa
- `400 Bad Request`: datos inválidos
- `403 Forbidden`: sin permisos o movimiento deshabilitado
- `404 Not Found`: recurso no encontrado cuando aplica
- `405 Method Not Allowed`: método HTTP no permitido
- `500 Internal Server Error`: error interno

## Reglas de visibilidad y permisos

### Productos

- Admin ve todos los productos.
- Usuario normal ve productos públicos y propios.
- Productos con `owner_user_id = null` son públicos.
- Productos con `owner_user_id != null` son visibles para su dueño y admin.

### Ventas y cambios

- Solo pueden operar productos visibles para el usuario actual.
- Si el producto no es visible para el usuario, la API responde error.
- Si `venta` o `cambio` están deshabilitados en configuración, la API responde `403`.

## Auditoría

Las escrituras por API generan eventos en `audit_events` con:
- `source = "api"`
- `integration_name` en payload cuando la autenticación entra por API key

Eventos actuales:
- `product_created`
- `product_assigned`
- `product_updated`
- `inventory_adjusted`
- `retoma_registered`
- `sale_registered`
- `credit_sale_created`
- `credit_installment_added`
- `change_registered`

## Recomendaciones para futuras integraciones

- Para integraciones reales, usa API key con `Authorization: Bearer`.
- Usa siempre `GET /api/products/search` antes de vender o cambiar un producto si dependes de búsqueda externa.
- Usa `GET /api/settings/lines` y `GET /api/settings/owners` antes de crear productos desde agentes o n8n.
- Consulta `/api/settings/business` para adaptar moneda, nombre y branding en integraciones externas.
- Si vas a integrar n8n, este documento puede convertirse luego en la base de una colección Postman o un OpenAPI.
