# API interna de Stocki App

Esta API permite consultar y operar Stocki App sin acceder directamente a la base de datos.

Estado actual:
- Base URL local: `http://localhost:8080`
- Formato: `application/json`
- Autenticación soportada:
  - sesión web existente
  - `Authorization: Bearer <token>`

## Autenticación

Los endpoints `/api/*` aceptan dos mecanismos durante la transición:

- sesión web válida
- API key vía header `Authorization: Bearer <token>`

Si no envías una sesión válida ni un Bearer token válido, la API responde:

```json
{
  "ok": false,
  "error": "Autenticación requerida para la API."
}
```

con código HTTP `401 Unauthorized`.

### Opción 1: sesión web

Útil para pruebas manuales.

Ejemplo para iniciar sesión y guardar cookies:

```bash
curl -c cookies.txt -X POST http://localhost:8080/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=SuperSecreto123"
```

Luego usa `-b cookies.txt` en las demás llamadas.

### Opción 2: API key

Las API keys se crean desde `Configuración > API keys`.

Reglas:
- el token solo se muestra al momento de creación
- en base de datos solo se guarda `token_hash`
- si la key está inactiva, deja de autenticar inmediatamente
- por ahora una API key válida opera con contexto admin controlado

Ejemplo de uso:

```bash
curl -H "Authorization: Bearer TU_TOKEN" http://localhost:8080/api/health
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

## Endpoints

### GET /api/health

Healthcheck JSON de la API. Requiere sesión o Bearer token válido.

Ejemplo con API key:

```bash
curl -H "Authorization: Bearer TU_TOKEN" http://localhost:8080/api/health
```

Respuesta:

```json
{
  "ok": true,
  "service": "stocki-app"
}
```

### GET /api/products

Lista productos visibles para el usuario autenticado.

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

Ejemplo:

```bash
curl -b cookies.txt http://localhost:8080/api/products
```

Ejemplo con API key:

```bash
curl -H "Authorization: Bearer TU_TOKEN" http://localhost:8080/api/products
```

### GET /api/products/search?q=

Busca productos visibles por `id`, nombre o línea.

Ejemplo:

```bash
curl -b cookies.txt "http://localhost:8080/api/products/search?q=crema"
```

Ejemplo con API key:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "http://localhost:8080/api/products/search?q=crema"
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
curl -X POST http://localhost:8080/api/products \
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

Ejemplo:

```bash
curl -b cookies.txt http://localhost:8080/api/inventory
```

Ejemplo con API key:

```bash
curl -H "Authorization: Bearer TU_TOKEN" http://localhost:8080/api/inventory
```

### GET /api/sales/recent

Devuelve las ventas recientes visibles para el usuario autenticado.

Ejemplo:

```bash
curl -b cookies.txt http://localhost:8080/api/sales/recent
```

Ejemplo con API key:

```bash
curl -H "Authorization: Bearer TU_TOKEN" http://localhost:8080/api/sales/recent
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

### POST /api/sales

Registra una venta. Respeta:
- stock disponible
- ownership
- métodos de pago activos
- tipo de movimiento `venta` habilitado

Payload:

```json
{
  "product_id": "P-001",
  "quantity": 1,
  "payment_method": "Efectivo",
  "unit_price": 25000,
  "notes": "Venta desde API"
}
```

También puedes enviar `total` en lugar de `unit_price`, o junto con él. Si ambos van presentes y `total > 0`, el backend usa `total / quantity` como precio unitario.

Ejemplo:

```bash
curl -X POST http://localhost:8080/api/sales \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TU_TOKEN" \
  -d '{
    "product_id": "P-001",
    "quantity": 1,
    "payment_method": "Efectivo",
    "unit_price": 25000,
    "notes": "Venta desde API"
  }'
```

Respuesta:

```json
{
  "ok": true,
  "product_id": "P-001",
  "product_name": "P-001",
  "quantity": 1,
  "message": "Venta registrada correctamente."
}
```

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
curl -X POST http://localhost:8080/api/swaps \
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

### GET /api/settings/business

Devuelve la configuración general del negocio.

Ejemplo:

```bash
curl -b cookies.txt http://localhost:8080/api/settings/business
```

Ejemplo con API key:

```bash
curl -H "Authorization: Bearer TU_TOKEN" http://localhost:8080/api/settings/business
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

## Endpoints para agente

Estos endpoints complementan la API general con respuestas más compactas para automatización y agentes tipo n8n.

Reglas:
- usan siempre `id` como identificador del producto
- respetan ownership y visibilidad actual
- aceptan sesión web o `Authorization: Bearer <token>`
- no generan auditoría en lecturas `GET`

### GET /api/agent/products/search?q=

Busca productos visibles por `id`, nombre o línea.

Ejemplo:

```bash
curl -H "Authorization: Bearer TU_TOKEN" "http://localhost:8080/api/agent/products/search?q=iphone"
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
curl -H "Authorization: Bearer TU_TOKEN" "http://localhost:8080/api/agent/products/price?id=IP12-001"
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
curl -H "Authorization: Bearer TU_TOKEN" "http://localhost:8080/api/agent/inventory?q=iphone"
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
curl -H "Authorization: Bearer TU_TOKEN" http://localhost:8080/api/agent/business
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

### GET /api/productos/precio

Endpoint legado para consultar el precio de venta de un producto por ID.

Uso:

```bash
curl -b cookies.txt "http://localhost:8080/api/productos/precio?id=P-001"
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
- `sale_registered`
- `change_registered`

## Recomendaciones para futuras integraciones

- Para pruebas rápidas, puedes seguir usando cookies.
- Para integraciones reales, usa API key con `Authorization: Bearer`.
- Usa siempre `GET /api/products/search` antes de vender o cambiar un producto si dependes de búsqueda externa.
- Consulta `/api/settings/business` para adaptar moneda, nombre y branding en integraciones externas.
- Si vas a integrar n8n, este documento puede convertirse luego en la base de una colección Postman o un OpenAPI.
