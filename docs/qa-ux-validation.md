# Matriz de validación UX/UI y accesibilidad — StockiAPP

Documento operativo para validar las mejoras de las Fases 0–7. No implementa telemetría ni modifica la aplicación.

## 1. Alcance y rutas

| Pantalla o flujo | Ruta real | Nota |
|---|---|---|
| Inventario | `/inventario` | Incluye venta, crédito, préstamo, retoma, ajuste, historial y “Más acciones”. |
| Clientes | `/clientes` | La búsqueda reutilizable usa `GET /api/customers`. |
| Venta | `/inventario`, `/venta/carrito`, `/venta/checkout` y POST `/venta` | Los productos se agregan al carrito desde Inventario; `/venta/new` solo conserva una redirección legacy. |
| Crédito | `/inventario` | Se prueba el modal/formulario de crédito desde la fila del producto. |
| Préstamo | `/prestamos/producto` y `/prestamos/producto/{id}` | También se inicia desde Inventario. |
| Retoma | `/inventario` | Se prueba desde la acción contextual de la fila. |
| Dashboard | `/dashboard` | Incluye filtros, métricas y eliminación de ventas para administradores. |
| Auditoría | `/auditoria` | Incluye filtros, tabla y estado vacío. |
| Configuración | `/configuracion` | Navegación por secciones mediante parámetros visibles. |
| Detalle de producto | `/inventario` + panel/modal de detalle | Validar breadcrumbs si el flujo abre una vista profunda. |
| Detalle de préstamo | `/prestamos/producto/{id}` | Incluye breadcrumbs, estado y acciones de cierre. |

Anchos de referencia: 320 px, 375 px, 768 px, 1024 px y escritorio amplio (1440 px o superior). En cada prueba usar zoom del navegador al 100 %, salvo que se indique lo contrario.

## 2. Matriz de pruebas por pantalla y dispositivo

Leyenda: **M** manual visual/flujo, **T** teclado, **A** automatizada.

| Pantalla | Dispositivo | Elemento a validar | Estado esperado | Tipo |
|---|---:|---|---|:---:|
| Inventario | 320 px | Tarjetas, nombre, ID, línea, stock, estado y precio | No hay scroll horizontal; datos esenciales visibles | M |
| Inventario | 375 px | Acción primaria y “Más acciones” | Acción frecuente a un toque; menú no sale de pantalla | M/T |
| Inventario | 768 px | Tabla/tarjetas y unidades | Tabla usable; unidades sin ancho forzado | M |
| Inventario | 1024 px | Tabla, filtros y acciones | Columnas legibles y controles sin solaparse | M |
| Inventario | 1440 px | Jerarquía visual y densidad | Tabla completa, acciones agrupadas y estados claros | M |
| Clientes | 320 px | Búsqueda y tarjetas | Campos ocupan el ancho; CTA accesible | M |
| Clientes | 375 px | Lookup asíncrono | Muestra Cargando, Sin resultados o error con Reintentar | M/T |
| Clientes | 768 px | Grid de clientes e importación | Tarjetas y panel de importación no desbordan | M |
| Clientes | 1024 px | Búsqueda, reset e importación | Flujo visible sin desplazamiento lateral | M |
| Clientes | 1440 px | Jerarquía y estados vacíos | Resultados y CTA se distinguen rápidamente | M |
| Venta | 320 px | Campos, precio y errores | Labels legibles; error asociado al campo | M/T |
| Venta | 375 px | Precio asíncrono | Carga, precio ausente y conexión fallida son distintos | M |
| Venta | 768 px | Formulario operativo | Campos y botón de envío cómodos para tablet | M |
| Venta | 1024 px | Validación y preservación de datos | Error no borra entradas; foco puede llegar al error | M/T |
| Venta | 1440 px | Jerarquía del formulario | Producto, cantidad, precio y pago tienen prioridad clara | M |
| Crédito | 320 px | Modal de crédito desde Inventario | Modal cabe en viewport; no scroll lateral; foco atrapado | M/T |
| Crédito | 375 px | Cliente, cuota y confirmación | Campos operables con touch y mensajes específicos | M |
| Crédito | 768 px | Modal y acciones de cuota | Controles agrupados; cierre por Escape | M/T |
| Crédito | 1024 px | Historial y confirmación | Historial legible; destructivas requieren contexto | M |
| Crédito | 1440 px | Estados de crédito | Activo, completado, suspendido y error diferenciables sin solo color | M |
| Préstamo | 320 px | Inicio/cierre desde Inventario | Modal o detalle usable sin desbordamiento | M/T |
| Préstamo | 375 px | Datos de cliente y retorno | Etiquetas explícitas; feedback de guardado visible | M |
| Préstamo | 768 px | Lista de préstamos | Tabla/lista y acciones accesibles por teclado | M/T |
| Préstamo | 1024 px | Detalle de préstamo | Breadcrumbs y acción Cerrar préstamo visibles | M |
| Préstamo | 1440 px | Historial y estado | Información operativa priorizada, sin ruido visual | M |
| Retoma | 320 px | Acción contextual y formulario | Campos apilados; confirmación no queda fuera del viewport | M/T |
| Retoma | 375 px | Mensajes de negocio | Precio/stock inválido explicado con acción correctiva | M |
| Retoma | 768 px | Modal y foco | Tab queda dentro; Escape cierra y restaura foco | T |
| Retoma | 1024 px | Menú de acciones | Retoma está dentro de Operar, no mezclada con Administrar | M |
| Retoma | 1440 px | Consistencia visual | Mismos botones, badges, radios y estados que Inventario | M |
| Dashboard | 320 px | Filtros y métricas | No se cortan tarjetas ni controles; contenido prioritario primero | M |
| Dashboard | 375 px | Carga asíncrona | Error de conexión no es silencioso y muestra toast accesible | M |
| Dashboard | 768 px | Tabla de ventas | Se puede revisar y operar sin layout roto | M |
| Dashboard | 1024 px | Eliminar venta | Confirmación contextual y feedback de éxito/error | M/T |
| Dashboard | 1440 px | Gráficos, resumen y timeline | Jerarquía y contraste correctos en light/dark | M/A |
| Auditoría | 320 px | Filtros y estado vacío | CTA “Limpiar filtros”; tabla con scroll contenido y anunciado | M |
| Auditoría | 375 px | Tabla horizontal contenida | Solo el contenedor desplaza; página no genera overflow global | M |
| Auditoría | 768 px | Filtros y tabla | Todos los campos y acciones alcanzables | M/T |
| Auditoría | 1024 px | Payload y navegación | Payload legible; breadcrumbs presentes si aplica | M |
| Auditoría | 1440 px | Contraste y densidad | Eventos, fuente y entidad distinguibles | M/A |
| Configuración | 320 px | Tabs/secciones | Navegación secundaria se adapta; sección activa identificable | M/T |
| Configuración | 375 px | Acordeones y opciones avanzadas | Avanzadas cerradas inicialmente; no desborde | M/T |
| Configuración | 768 px | Formularios de negocio/inventario | Secciones separadas y CTA visibles | M |
| Configuración | 1024 px | Métodos de pago/usuarios | Acceso directo por parámetro; estado vacío con CTA | M |
| Configuración | 1440 px | Integraciones/avanzado | Opciones destructivas advertidas y protegidas | M/T |
| Detalle de producto | 320 px | Panel/modal de detalle | Lectura vertical; cierre y foco operables | M/T |
| Detalle de producto | 375 px | Datos esenciales | Nombre, ID, stock, estado y precio no se ocultan | M |
| Detalle de producto | 768 px | Unidades | Lista o expansión usable sin tabla ancha | M |
| Detalle de producto | 1024 px | Breadcrumbs y acciones | Ruta de orientación navegable; acciones agrupadas | M/T |
| Detalle de producto | 1440 px | Sistema visual | Tokens, tipografía y estados consistentes | M/A |
| Detalle de préstamo | 320 px | Detalle y cierre | Sin scroll horizontal; botón de cierre accesible | M/T |
| Detalle de préstamo | 375 px | Breadcrumbs | Ruta completa visible o desplazable dentro del componente | M |
| Detalle de préstamo | 768 px | Unidades y cliente | Datos etiquetados y orden lógico | M |
| Detalle de préstamo | 1024 px | Estado y acción principal | Cerrar préstamo es claro y requiere confirmación si aplica | M/T |
| Detalle de préstamo | 1440 px | Lectura y contraste | Información secundaria no compite con la acción principal | M/A |

### Criterio transversal por ancho

- 320/375 px: cero overflow horizontal del `body`, targets de al menos 44 × 44 px y textos sin truncamiento crítico.
- 768 px: experiencia tablet-first, tabla o tarjeta sin pérdida de información esencial.
- 1024 px: tabla y paneles aprovechan el espacio sin columnas comprimidas ilegibles.
- 1440 px: no aumentar innecesariamente la densidad; mantener una jerarquía clara y un solo CTA primario por contexto.

## 3. Checklist de teclado y lectores de pantalla

Ejecutar con teclado físico, sin usar el mouse. Repetir en Chrome/Firefox y, cuando sea posible, con NVDA (Windows) o VoiceOver (macOS/iOS).

### Checklist general

- [ ] El foco visible tiene contraste suficiente y nunca desaparece.
- [ ] `Tab` recorre controles en orden visual y lógico.
- [ ] `Shift+Tab` retrocede sin saltos inesperados.
- [ ] `Enter` activa enlaces, botones, tabs y la acción primaria esperada.
- [ ] `Escape` cierra modal, menú móvil o menú “Más acciones” activo.
- [ ] Al cerrar un modal, el foco vuelve al botón que lo abrió.
- [ ] Un modal abierto impide que `Tab` escape al contenido de fondo.
- [ ] Los cambios de carga, éxito y error se anuncian por `aria-live`/`role`.
- [ ] Ningún estado depende solo del color: tiene texto o icono equivalente.

### Componentes modificados

| Componente | Pruebas específicas | Aceptación |
|---|---|---|
| Modales de Inventario/Crédito/Préstamo/Retoma | Abrir con Enter; primer control recibe foco; Tab/Shift+Tab quedan dentro; Escape cierra | Focus trap funcional y foco restaurado |
| Menú móvil | Abrir con Enter/Space; foco va al primer enlace; recorrer items; Escape; cerrar con botón | `aria-expanded`/`aria-hidden` coherentes, body sin scroll y foco restaurado |
| “Más acciones” | Abrir, recorrer Operar → Consultar → Administrar, activar una acción | Orden lógico, cierre con Escape y destructivas distinguibles |
| Tabs de Configuración | Tab hasta navegación; Enter en sección; recorrer opciones avanzadas | Sección activa visible y anunciada; URL bookmarkable |
| Breadcrumbs | Tab por cada nivel; Enter en cada enlace | Cada nivel salvo el actual es navegable |
| Formularios | Tab por labels, ayuda, campo y error; provocar validación | Valores preservados y error asociado al campo |
| Estados/toasts | Provocar carga, éxito, error y vacío; cerrar toast por teclado | Mensaje anunciado; botón de cierre accesible; no se pierde el contexto |

## 4. Pruebas automatizadas de accesibilidad

### axe-core con Playwright

No hay runner de navegador incluido actualmente en el repositorio. En una máquina con Node.js:

```bash
npm install --save-dev @playwright/test axe-core
npx playwright install chromium
```

Crear temporalmente un test local (fuera del commit de la aplicación) que visite cada ruta autenticada y ejecute `new AxeBuilder({ page }).analyze()`. Ejemplo de ejecución:

```bash
npx playwright test accessibility.spec.js --project=chromium
```

Rutas mínimas: `/inventario`, `/venta/carrito`, `/venta/checkout`, `/clientes`, `/dashboard`, `/auditoria`, `/configuracion`, `/prestamos/producto` y un detalle de préstamo con datos de prueba.

### Lighthouse

Con la aplicación levantada y autenticada:

```bash
npx lighthouse http://localhost:8092/inventario \
  --only-categories=accessibility,performance \
  --view
```

Repetir para Configuración, Clientes, Venta y Detalle de préstamo. Lighthouse requiere una sesión válida si las rutas están protegidas.

### WAVE

Usar la extensión WAVE sobre las mismas rutas o el servicio interno aprobado por el equipo. Revisar especialmente formularios, modales abiertos, tablas y estados dinámicos.

### Criterios de aceptación

- 0 violaciones **critical** y 0 violaciones **serious** en axe-core.
- No dejar errores WAVE de estructura, labels, contraste o ARIA.
- Lighthouse Accessibility ≥ 90 como objetivo inicial; cualquier excepción debe documentarse.
- Contraste mínimo WCAG 2.1 AA: 4.5:1 para texto normal y 3:1 para texto grande/componentes gráficos relevantes.
- Toda operación asíncrona debe tener carga, resultado o error anunciado.

## 5. Guía de pruebas con usuarios operativos

Realizar con 5–8 personas que conozcan la operación diaria. No explicar la solución durante el primer intento. Registrar pantalla, dispositivo, rol, experiencia previa y si usa teclado o touch.

### Flujo 1 — Registrar una venta

**Objetivo:** completar una venta desde Inventario sin buscar acciones secundarias innecesarias.

**Pasos esperados:** abrir `/inventario`; localizar un producto disponible; usar la acción primaria “Vender”; seleccionar/registrar cliente si aplica; confirmar cantidad y precio; guardar.

**Observar:** si el estado del stock se entiende antes de abrir el menú; si el precio se carga; si los errores de stock/precio son accionables; si el feedback de éxito es visible.

**Métricas:** tiempo desde producto localizado hasta confirmación; errores de cantidad/precio; aperturas de “Más acciones”; abandono antes de guardar.

### Flujo 2 — Ajustar inventario

**Objetivo:** corregir unidades de un producto con confirmación clara.

**Pasos esperados:** abrir Inventario; elegir “Ajustar” en “Más acciones” → Administrar; introducir cantidad/motivo; confirmar; revisar toast/banner.

**Observar:** dificultad para encontrar Ajustar; comprensión de la consecuencia; foco del modal; mensaje ante error de negocio.

**Métricas:** tiempo hasta abrir Ajustar; número de pasos; errores de cantidad; cancelaciones; reintentos.

### Flujo 3 — Crear un producto

**Objetivo:** completar solo la información necesaria y conservar datos si hay validación.

**Pasos esperados:** abrir `/productos/new`; completar ID, nombre, cantidad, línea, precio y “Requiere talla” si corresponde; dejar avanzadas cerradas; enviar; provocar opcionalmente un error y corregirlo.

**Observar:** comprensión de campos esenciales; utilidad de ejemplos; descubrimiento de “Opciones avanzadas”; preservación de valores y foco en errores.

**Métricas:** tiempo hasta envío válido; campos consultados; errores por campo; expansión de avanzadas; abandono del formulario.

### Flujo 4 — Registrar un crédito

**Objetivo:** registrar crédito sin confundirlo con una venta de contado.

**Pasos esperados:** desde Inventario elegir Crédito/Operar; seleccionar cliente y producto; completar cuota/plazo; revisar resumen; confirmar.

**Observar:** comprensión de deuda, cuota y estado; carga de cliente; mensajes de cliente no encontrado o stock insuficiente; foco de confirmación.

**Métricas:** tiempo total; errores de cliente/cuota; búsquedas repetidas; reintentos; abandono del modal.

### Flujo 5 — Buscar un cliente

**Objetivo:** reutilizar un cliente existente en un flujo operativo.

**Pasos esperados:** activar búsqueda; escribir al menos dos caracteres; esperar “Cargando”; seleccionar resultado; verificar que los campos se completen.

**Observar:** comprensión del formato de búsqueda; diferencia entre Sin resultados y No se pudo conectar; uso de Reintentar; foco en resultados.

**Métricas:** tiempo hasta selección; consultas por búsqueda; resultados descartados; reintentos por conexión; porcentaje que crea/abandona.

### Flujo 6 — Cerrar un préstamo

**Objetivo:** localizar un préstamo y cerrarlo con seguridad.

**Pasos esperados:** abrir `/prestamos/producto`; localizar préstamo; abrir detalle; revisar cliente, unidades y retorno; elegir Cerrar préstamo; confirmar.

**Observar:** facilidad para orientarse con breadcrumbs; comprensión del estado; visibilidad de la acción principal; confirmación destructiva y feedback.

**Métricas:** tiempo hasta detalle; errores de identificación; cancelaciones; reintentos; éxito al primer intento.

## 6. Plantilla de registro de métricas

| Fecha | Participante/rol | Flujo | Dispositivo/ancho | Tiempo (s) | Errores | Reintentos | Abandono (S/N) | Uso “Más acciones” | Observación |
|---|---|---|---:|---:|---:|---:|:---:|---:|---|
| | | | | | | | | | |

### Indicadores agregados

| Métrica | Fórmula | Línea base | Objetivo posterior |
|---|---|---:|---:|
| Tiempo de operación | mediana de segundos por flujo | — | Reducir frente a la primera ronda |
| Errores por flujo | errores / intentos | — | Tendencia descendente |
| Uso de Más acciones | sesiones que lo abren / sesiones | — | Validar que acciones frecuentes no dependan de él |
| Abandono de formulario | formularios abandonados / iniciados | — | Reducir frente a línea base |
| Reintentos por error | reintentos / errores de conexión | — | Medir recuperación, no penalizar automáticamente |
| Éxito al primer intento | operaciones completas sin corrección / intentos | — | Aumentar por flujo |

La línea base queda vacía porque no se puede ejecutar la aplicación con datos reales en el entorno actual. Registrar al menos una ronda inicial antes de comparar mejoras.

## 7. Configuración del entorno de pruebas

StockiAPP requiere PostgreSQL; SQLite no es compatible.

### Opción local con Docker

```bash
docker run --name stockiapp-postgres \
  -e POSTGRES_USER=stockiapp \
  -e POSTGRES_PASSWORD=stockiapp \
  -e POSTGRES_DB=stockiapp_dev \
  -p 5432:5432 -d postgres:16

export DATABASE_URL='postgres://stockiapp:stockiapp@127.0.0.1:5432/stockiapp_dev?sslmode=disable'
export ADMIN_USER=admin
export ADMIN_PASS='cambiar-esta-clave'
export PORT=8092
go run .
```

Para detener y eliminar el contenedor de prueba:

```bash
docker stop stockiapp-postgres
docker rm stockiapp-postgres
```

También se puede usar `DB_DSN` en lugar de `DATABASE_URL`:

```bash
export DB_DSN="$DATABASE_URL"
```

### Ejecutar pruebas Go

Validación de compilación sin inicializar base de datos:

```bash
go test -run '^$' ./...
```

Con PostgreSQL configurado y accesible:

```bash
go test ./...
```

La suite existente contiene 119 pruebas según el inventario de la fase. Si falla por `DB_DSN o DATABASE_URL es obligatorio`, el problema es de configuración del entorno, no un resultado funcional de la suite.

### Levantar la aplicación para pruebas manuales

```bash
go run .
```

Abrir `http://localhost:8092/inventario` y usar las credenciales definidas en `ADMIN_USER`/`ADMIN_PASS`.

## 8. Reporte de defectos

Cada hallazgo debe incluir:

1. Pantalla y ruta.
2. Ancho/dispositivo, navegador y rol.
3. Pasos reproducibles.
4. Resultado esperado y resultado actual.
5. Severidad: crítica, alta, media o baja.
6. Evidencia: captura, video, salida axe/Lighthouse o log.
7. Si afecta teclado, lector de pantalla, contraste, responsive o feedback.

Priorizar primero bloqueos de operación, pérdida de datos, foco inaccesible, violaciones WCAG serias y ausencia de recuperación ante errores.
