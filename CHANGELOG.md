# Changelog

## 2026-03-23

### API e integraciones
- Se consolidaron endpoints API para automatización e integraciones externas desde `main.go`, manteniendo el patrón real del proyecto.
- Quedó soporte operativo para:
  - `GET /api/inventory`
  - `POST /api/inventory/adjust`
  - `GET /api/retomas`
  - `POST /api/retomas`
  - `GET /api/sales/recent`
  - `GET /api/sales`
  - `POST /api/sales`
  - `GET /api/settings/lines`
  - `GET /api/settings/owners`
- La API de ventas se ajustó para integraciones tipo n8n con soporte de lectura y escritura más completo.
- Se mantuvo compatibilidad con ownership, validaciones de stock, movimientos habilitados y auditoría existente.

### Auditoría y consistencia
- Se reforzó el uso del patrón de respuesta y errores API con `writeAPIJSON(...)` y `writeAPIError(...)`.
- Las operaciones de inventario, retoma y ventas vía API quedaron alineadas con metadata de auditoría y `source = "api"`.
- Se mantuvo el backend canónico sin renombrar conceptos internos del dominio.

### Documentación
- `docs/api.md` quedó reorganizado y ampliado como fuente principal para integraciones.
- Se documentaron ejemplos `curl` orientados a producción con base pública `https://login.stockiapp.co`.
- Se añadieron contratos y ejemplos para ventas, retomas, ajuste de inventario y catálogos de configuración.

### UI visual
- Se refinó la capa visual de la app sin tocar lógica de negocio ni API.
- Se mejoraron jerarquía, tipografía, superficies, navegación superior y lectura operativa en dashboard e inventario.
- Se mantuvo el enfoque tablet-first y SSR.

### Gobierno del proyecto
- `AGENTS.md` fue actualizado para reflejar:
  - el patrón real de API en `main.go`
  - los helpers canónicos a reutilizar
  - los endpoints ya consolidados
  - reglas para documentación viva en `docs/api.md`
  - criterios explícitos para tareas visuales sin impacto funcional
