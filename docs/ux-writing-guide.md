# Guía de UX Writing y auditoría de microcopy — StockiAPP

## 1. Guía de tono y voz

### Principios

- **Claro:** usar palabras cotidianas y nombrar la entidad concreta.
- **Directo:** una idea por frase; eliminar introducciones y explicaciones obvias.
- **Accionable:** todo error indica qué puede hacer la persona después.
- **Neutral y respetuoso:** describir el problema sin culpar al usuario.
- **Consistente:** conservar los términos del glosario en pantallas, modales y API visible.
- **Operativo:** priorizar escaneo rápido sobre textos editoriales extensos.

### Tono por contexto

| Contexto | Tono | Ejemplo |
|---|---|---|
| Operación | Breve y activo | “Registrar venta” |
| Confirmación | Claro y tranquilizador | “Se eliminará el producto Camiseta. Esta acción no se puede deshacer.” |
| Error | Explicativo, sin culpar | “No se pudo guardar. Revisa tu conexión e inténtalo de nuevo.” |
| Error de negocio | Específico y útil | “Solo hay 5 unidades disponibles. Reduce la cantidad o ajusta el inventario.” |
| Estado vacío | Orientador y motivador | “Aún no hay productos. Crea el primero para comenzar.” |
| Éxito | Confirmación concreta | “Producto creado.” |
| Carga | Descriptivo | “Cargando clientes…” |

### Reglas lingüísticas

- Usar **tú** de forma consistente; evitar alternar con “usted”.
- Botones con verbo en infinitivo: “Guardar”, “Crear producto”, “Registrar venta”, “Eliminar producto”.
- Evitar “OK”, “Aceptar”, “Continuar” y “Confirmar” cuando puede nombrarse la acción.
- Usar oración tipo frase: solo mayúscula inicial, salvo nombres propios.
- Usar tildes y signos de interrogación/apertura correctamente.
- Terminar mensajes completos con punto; no añadir punto a labels y botones salvo que sean frases largas.
- Preferir 2–8 palabras en botones, 8–16 en títulos y 1–2 frases en ayudas.
- Un texto de ayuda debe existir solo si aporta formato, diferencia entre opciones o una consecuencia no evidente.
- El placeholder muestra un ejemplo; nunca reemplaza al `<label>`.
- Los iconos no sustituyen el texto accesible.

## 2. Glosario canónico

| Término | Uso recomendado | Evitar |
|---|---|---|
| Producto | Registro comercial del catálogo | Artículo, ítem, mercancía |
| Unidad | Existencia física individual | Pieza, elemento, registro |
| Línea | Clasificación del producto | Categoría, grupo, sección |
| Cliente | Persona asociada a una operación | Deudor, comprador, usuario |
| Usuario | Persona con acceso a StockiAPP | Cliente, operador, cuenta cuando se habla de personas |
| Venta | Salida definitiva de producto contra pago | Sale, operación, transacción |
| Crédito | Venta o préstamo con saldo/cuotas | Fiado, deuda, financiación, salvo contexto financiero explícito |
| Préstamo | Entrega temporal de producto o dinero | Loan, salida temporal |
| Retoma | Recepción de un producto entregado por el cliente | Recompra, devolución, trade-in |
| Cambio | Intercambio de un producto por otro | Swap, reemplazo |
| Ajuste | Corrección controlada de existencias | Movimiento manual, cambio de stock |
| Inventario | Conjunto de productos y unidades | Stock, salvo cuando se habla de cantidad disponible |
| Stock | Cantidad disponible de unidades | Inventario, cuando se necesita expresar una cantidad |
| ID | Identificador visible único del producto | SKU, código interno |
| Locación | Ubicación física del producto | Ubicación solo si el negocio lo prefiere; no alternar ambas |

## 3. Cambios de microcopy aplicados

| Archivo | Antes | Después | Motivo |
|---|---|---|---|
| `templates/product_new.html` | “Identificador visible y único. Ej: PROD-001.” | “Ej: PROD-001.” | El label “ID” ya explica la función. Se conserva el formato útil. |
| `templates/product_new.html` | “Número de unidades que ingresarán al inventario.” | “Ej: 10.” | El label y tipo numérico explican el campo; queda solo el ejemplo. |
| `templates/product_new.html` | “Usa un nombre corto y fácil de encontrar en Inventario.” | Eliminado | Repite el propósito obvio del campo Nombre. |
| `templates/product_new.html` | “Precio mostrado para la venta. Ej: 999.99.” | “Ej: 999.99.” | Se elimina la explicación redundante y se conserva el formato. |
| `templates/product_new.html` | “Referencia física para encontrar el producto rápidamente.” | “Ej: Estante A-03.” | Se conserva el ejemplo y se reduce la explicación. |
| `templates/product_new.html` | “Valor que se reconocerá al recibir el producto de vuelta.” | “Valor reconocido al recibir el producto.” | Misma consecuencia, con menos palabras. |
| `templates/product_new.html` | “Guardar producto” | “Crear producto” | La pantalla crea un registro nuevo; el verbo es más preciso. |
| `templates/venta_new.html` | “Valor venta final (opcional)” | “Valor final de la venta (opcional)” | Etiqueta natural y sin omisión gramatical. |
| `templates/venta_new.html` | “Confirmar” | “Registrar venta” | La acción queda explícita. |
| `templates/cambio_new.html` | “Sale” | “Producto entregado” | Elimina jerga técnica expuesta y describe la entidad. |
| `templates/cambio_new.html` | “Confirmar” | “Registrar cambio” | Acción concreta. |
| `templates/audit_events.html` | “Creditos editados” | “Créditos editados” | Corrección ortográfica. |
| `templates/audit_events.html` | “Prestamos fisicos” | “Préstamos físicos” | Corrección ortográfica. |

## 4. Auditoría de textos de ayuda

Clasificación: **mantener** = aporta información no obvia; **simplificar** = conservar con menos palabras; **eliminar** = label/placeholder suficiente; **mover a tooltip** = información secundaria o avanzada.

| Archivo/ubicación | Texto revisado | Clasificación | Acción |
|---|---|---|---|
| `product_new.html`, ID | “Identificador visible y único. Ej: PROD-001.” | Simplificar | Queda “Ej: PROD-001.” |
| `product_new.html`, Cantidad | “Número de unidades que ingresarán al inventario.” | Simplificar | Queda “Ej: 10.” |
| `product_new.html`, Nombre | “Usa un nombre corto y fácil de encontrar en Inventario.” | Eliminar | El label, placeholder y contexto de la pantalla son suficientes. |
| `product_new.html`, Talla | “Si escribes una talla, el sistema entiende que el producto la requiere. Déjala vacía si no aplica.” | Mantener | Regla de negocio no evidente; conservar en una frase. |
| `product_new.html`, Precio de venta | “Precio mostrado para la venta. Ej: 999.99.” | Simplificar | Queda “Ej: 999.99.” |
| `product_new.html`, Locación | “Referencia física para encontrar el producto rápidamente.” | Simplificar | Queda “Ej: Estante A-03.” |
| `product_new.html`, Valor de retoma | “Valor que se reconocerá al recibir el producto de vuelta.” | Simplificar | Queda “Valor reconocido al recibir el producto.” |
| `product_new.html`, Fecha de caducidad | “Usa la fecha de vencimiento indicada por el proveedor.” | Mantener | Consecuencia/formato operativo no obvio. |
| `venta_new.html`, Valor final | “Si lo llenas, este valor reemplaza (Precio unitario x Cantidad).” | Simplificar | Recomendado: “Opcional. Reemplaza el total calculado.” |
| `venta_new.html`, stock | “Disponibles: …” | Mantener | Estado dinámico esencial para decidir cantidad. |
| `inventario.html`, búsqueda | “Nombre, ID, línea, deudor o documento” | Mantener | Explica campos que la búsqueda acepta; es un placeholder, no ayuda permanente. |
| `inventario.html`, cliente | “Busca por nombre, documento o teléfono para reutilizar un cliente.” | Simplificar | Recomendado: “Busca por nombre, documento o teléfono.” |
| `inventario.html`, campos de cliente | “Nombre del cliente”, “Documento del cliente”, etc. | Eliminar como ayuda | Son placeholders de ejemplo; el label ya identifica cada campo. |
| `customers.html`, búsqueda | “Nombre, documento, teléfono o ciudad” | Mantener | Especifica criterios de búsqueda válidos. |
| `customers.html`, importación | “Procesando archivo, espera unos segundos…” | Mantener | Comunica que la operación sigue activa. |
| `product_loan_detail.html`, nota operativa | Texto de seguimiento del préstamo | Mantener | Contextualiza una acción no obvia; revisar en futuras pruebas si no se consulta. |
| `business_settings.html`, ejemplos de branding | Ejemplos de teléfono, email y redes | Mantener | Formato y contenido esperado no son uniformes para todos los negocios. |
| `business_settings.html`, API keys | Descripciones extensas sobre integración | Mover a tooltip/enlace | La operación principal es crear/rotar la key; documentación técnica debe vivir fuera del formulario. |
| `audit_events.html`, payload | Explicación del JSON | Eliminar | El contenido visible del payload y el encabezado son suficientes para usuarios administradores. |
| `dashboard.html`, filtros | Descripciones de rango y gráficos | Simplificar | Mantener solo el contexto necesario para interpretar el rango seleccionado. |
| `cambio_new.html`, observaciones | “Observaciones” | Eliminar ayuda adicional | Label y textarea son suficientes. |

## 5. Patrones de mensajes

### Validación de campo

Patrón: **[Campo] + [problema] + [corrección]**.

- “El ID es obligatorio. Ej: PROD-001.”
- “La cantidad debe ser mayor que 0.”
- “Ingresa un precio válido, por ejemplo 999.99.”

### Permisos

Patrón: **acción no disponible + razón breve + alternativa**.

- “No tienes permiso para editar este producto. Solicita acceso a un administrador.”
- “Solo personal autorizado puede importar clientes.”

### Conexión

Patrón: **acción que falló + recuperación**.

- “No se pudo cargar el precio. Revisa tu conexión e inténtalo de nuevo.”
- “No se pudo buscar clientes. Reintenta la búsqueda.”

### Error de negocio

Patrón: **regla concreta + siguiente acción**.

- “Solo hay 5 unidades disponibles. Reduce la cantidad o ajusta el inventario.”
- “Este producto no tiene precio configurado. Ingresa un precio para continuar.”
- “El cliente no fue encontrado. Revisa los datos o registra uno nuevo.”

### Éxito, carga y vacío

- Éxito: “Producto creado.” / “Venta registrada.” / “Ajuste guardado.”
- Carga: “Cargando productos…” / “Cargando historial…” / “Buscando clientes…”
- Vacío: “Aún no hay productos. Crea el primero para comenzar.”
- Sin resultados: “No hay resultados con estos filtros. Limpia los filtros o cambia la búsqueda.”

## 6. Confirmaciones destructivas

Usar siempre esta estructura:

1. Título: **“Eliminar producto”**.
2. Contexto: **“Se eliminará Camiseta básica (PROD-001). Esta acción no se puede deshacer.”**
3. Acción secundaria: **“Cancelar”**.
4. Acción destructiva: **“Eliminar producto”**.

Evitar títulos y botones aislados como “Confirmar”, “Aceptar” o “¿Está seguro?”.

## 7. Reglas para toasts y estados

- Nunca usar “Éxito”, “Error” u “Operación completada” sin nombrar la acción.
- Un toast de éxito debe responder qué ocurrió: “Cliente actualizado.”
- Un toast de error debe responder qué falló y qué hacer: “No se pudo guardar. Revisa tu conexión e inténtalo de nuevo.”
- Los mensajes de estado deben ser comprensibles sin el color, icono o animación.
- No repetir en un toast el texto completo del formulario; resumir y dejar el error de campo junto al campo.
- Mantener la opción de cierre accesible y no ocultar errores críticos automáticamente sin alternativa.

## 8. Auditoría pendiente por pantalla

| Pantalla | Prioridad editorial | Revisión siguiente |
|---|---:|---|
| Inventario | Alta | Sustituir mensajes “No se pudo…” por la clasificación validación/conexión/negocio y revisar ayudas repetidas en modales. |
| Configuración | Alta | Recortar descripciones largas de tenants, API keys y movimientos; mover detalles técnicos a ayuda secundaria. |
| Producto nuevo/edición | Alta | Mantener solo formato, talla, caducidad y consecuencias no obvias. |
| Venta | Alta | Reemplazar “Confirmar” restante y simplificar explicación del total final. |
| Clientes | Media | Mantener criterios de búsqueda; acortar mensajes de importación sin perder estados. |
| Dashboard | Media | Convertir errores silenciosos en mensajes accionables y revisar títulos de métricas. |
| Auditoría | Media | Mantener nombres técnicos de eventos en datos, pero usar labels comprensibles en navegación. |
| Detalle de préstamo | Media | Revisar textos de seguimiento después de observar usuarios reales. |
| Cambio | Alta | Continuar reemplazando términos técnicos como `SKU`, `Sale` o `incoming` cuando aparezcan en UI. |

## 9. Patrón para futuros desarrollos

Antes de añadir un texto, responder:

1. ¿El label ya comunica esto?
2. ¿El texto aporta formato, diferencia o consecuencia?
3. ¿Puede decirse en una sola frase?
4. ¿La persona sabe qué hacer después?
5. ¿El texto funciona leído por un lector de pantalla sin depender del icono?

Si el label y el placeholder bastan, no añadir ayuda visible. Si la explicación es técnica o extensa, llevarla a documentación, tooltip accesible o enlace “Saber más”.
