# Guía Completa: Dashboard en Looker Studio (Google Data Studio)

Este documento cubre **todo el flujo** para construir el dashboard del workshop: desde exponer MySQL con ngrok, crear el usuario de Looker, conectar la fuente de datos, hasta el paso a paso de cada una de las 8 gráficas con nombre exacto, query, dimensiones, métricas y estilo.

---

## Requisitos previos

- Infraestructura levantada con `docker compose up -d` (contenedores Zookeeper, Kafka y MySQL corriendo).
- Schema MySQL inicializado automáticamente desde `sql/create_tables.sql` (sucede en el primer arranque del contenedor).
- ngrok instalado y autenticado.
- Cuenta Google para Looker Studio.
- Al menos un run de streaming completo (`python kafka/consumer.py` + `python kafka/producer.py`) para que las tablas tengan datos.
- Tener `sql/kpis.sql` abierto en paralelo para copiar las 8 queries.

---

# PARTE A — Setup de la fuente de datos

## Paso 1 — Exponer MySQL con ngrok

1. En una terminal nueva: `ngrok tcp 3307` (el contenedor MySQL publica el puerto host 3307 → contenedor 3306).
2. Copia el host:port que devuelve ngrok (ej. `0.tcp.ngrok.io:14523`).

## Paso 2 — Crear usuario read-only para Looker

SQL a ejecutar:

```sql
CREATE USER 'looker'@'%' IDENTIFIED BY 'looker_pwd_changeme';
GRANT SELECT ON happiness.* TO 'looker'@'%';
FLUSH PRIVILEGES;
```

Comando:

```bash
docker exec -i ws3_mysql mysql -uroot -p$MYSQL_ROOT_PASSWORD < grant_looker.sql
```

## Paso 3 — Conectar Looker Studio a MySQL

1. Abrir `lookerstudio.google.com` → **Create** → **Data source** → **MySQL**.
2. Completar:
   - **Host:** `0.tcp.ngrok.io`
   - **Port:** el que devolvió ngrok (ej. `14523`)
   - **Database:** `happiness`
   - **User:** `looker`
   - **Password:** `looker_pwd_changeme`
3. Click **Authenticate** → **Connect**.
4. Renombrar la fuente a `happiness@ngrok` para usarla en las gráficas.

---

# PARTE B — Construcción de las 8 gráficas

## Convención general para las 8 gráficas

Para **cada** KPI, el flujo en Looker Studio es:

1. **Insert → Chart** y elegir el tipo indicado en la tabla.
2. En el panel **DATA** (derecha), click en la fuente de datos actual → **+ Add data source** (si aún no está conectada) → o seleccionar la fuente `happiness@ngrok`.
3. Al elegir la tabla, marcar la opción **CUSTOM QUERY** y pegar la query correspondiente de `sql/kpis.sql`.
4. Click **ADD** → Looker mostrará los campos detectados.
5. Configurar **Dimension** y **Metric** según se indica en cada sección.
6. Pasar al panel **STYLE** y aplicar las recomendaciones de estilo.
7. Renombrar el chart haciendo doble click en el header → escribir el **nombre exacto** indicado.

> **Paleta global recomendada:**
> - Rojo errores: `#D32F2F`
> - Verde aciertos / VALID: `#388E3C`
> - Naranja advertencia / INVALID_VALUES: `#F57C00`
> - Gris neutro / INVALID_SCHEMA: `#616161`
> - Morado predicción / PREDICTION_ERROR: `#7B1FA2`
> - Azul referencia: `#1976D2`

---

## KPI 1 — `MAE Global` (Scorecard)

**Propósito:** mostrar de un solo vistazo qué tan lejos están las predicciones del valor real, promediadas sobre todos los eventos VALID.

**Configuración en Looker Studio:**

| Campo | Valor |
|---|---|
| **Chart type** | Scorecard |
| **Nombre del componente** | `MAE Global` |
| **Custom Query** | `SELECT AVG(ABS(prediction_error)) AS mae_global FROM fact_predictions;` |
| **Metric** | `mae_global` (tipo Number, agregación `Auto` o `Average` — la query ya agrega) |
| **Dimension** | _(ninguna)_ |
| **Comparison metric** | desactivada |

**Estilo:**
- Decimal places: `3`.
- Number format: `Number`.
- Label: `MAE`.
- Color del número: `#D32F2F` si el valor > 0.5, `#388E3C` si ≤ 0.5 (usar **Conditional formatting** → "Single color" → rule `mae_global > 0.5`).
- Background: blanco. Border: 1 px gris.

**Lectura esperada:** un único número alrededor de `0.4–0.5` (valor offline en el modelo entrenado).

---

## KPI 2 — `Predicciones por País` (Bar chart horizontal)

**Propósito:** ver qué países han recibido más eventos procesados con éxito.

**Configuración en Looker Studio:**

| Campo | Valor |
|---|---|
| **Chart type** | Bar chart (horizontal) |
| **Nombre del componente** | `Predicciones por País` |
| **Custom Query** | KPI 2 de `sql/kpis.sql` |
| **Dimension** | `country_name` |
| **Metric** | `n_predictions` (SUM) |
| **Sort** | `n_predictions` descendente |
| **Rows per page** | 20 |

**Estilo:**
- Single color bars: `#1976D2`.
- Mostrar data labels al final de cada barra.
- Y-axis label: `País`. X-axis label: `# eventos VALID`.
- Eje X con grid lines gris claro.

**Lectura esperada:** los países con más años cubiertos (potencialmente hasta 5: 2015–2019) aparecen primero.

---

## KPI 3 — `Predicho vs Actual` (Scatter plot)

**Propósito:** validar visualmente que el modelo no diverge del valor real; idealmente los puntos caen sobre la diagonal `y = x`.

**Configuración en Looker Studio:**

| Campo | Valor |
|---|---|
| **Chart type** | Scatter chart |
| **Nombre del componente** | `Predicho vs Actual` |
| **Custom Query** | KPI 3 de `sql/kpis.sql` |
| **Dimension** | _(ninguna; cada fila es un punto)_ |
| **Metric X** | `actual_score` |
| **Metric Y** | `predicted_score` |
| **Bubble size** | _(ninguna)_ |

**Estilo:**
- Point shape: círculo, tamaño 6 px.
- Color: `#388E3C` con opacidad 50 %.
- Activar **Trendline** → Linear → color `#D32F2F`, grosor 2 px.
- Ejes con mismos límites: min `0`, max `10`, intervalo `1`.
- Habilitar **Show axis title** en ambos ejes (`Happiness real`, `Happiness predicha`).

**Lectura esperada:** nube de puntos a 45° con dispersión simétrica; la línea de tendencia debe quedar prácticamente sobre la diagonal.

---

## KPI 4 — `Tendencia Diaria de Predicciones` (Time series)

**Propósito:** evidenciar que el pipeline corre en el tiempo y comparar promedio predicho vs promedio real día a día.

**Configuración en Looker Studio:**

| Campo | Valor |
|---|---|
| **Chart type** | Time series |
| **Nombre del componente** | `Tendencia Diaria de Predicciones` |
| **Custom Query** | KPI 4 de `sql/kpis.sql` |
| **Time Dimension** | `day` (tipo Date) |
| **Metrics** | `avg_predicted`, `avg_actual` (dos series) |
| **Sort** | `day` ascendente |

**Estilo:**
- Serie `avg_predicted`: línea sólida color `#1976D2`, ancho 2 px, puntos visibles.
- Serie `avg_actual`: línea discontinua (dashed) color `#388E3C`, ancho 2 px.
- Mostrar leyenda al fondo (Bottom).
- Y-axis: min `0`, max `10`.

**Lectura esperada:** ambas líneas se mueven muy juntas; brechas grandes en algún día indican lotes con países atípicos.

> **Nota:** si todo el streaming corre en un solo día, esta gráfica colapsa a un único punto. En ese caso, cambiar el `DATE()` por `DATE_FORMAT(prediction_timestamp, '%Y-%m-%d %H:%i')` en `sql/kpis.sql` para granularidad por minuto y volver a importar la query.

---

## KPI 5 — `Top 10 Países con Mayor Error` (Bar chart horizontal)

**Propósito:** identificar los países donde el modelo se equivoca más en promedio (foco para iterar features o mapeo).

**Configuración en Looker Studio:**

| Campo | Valor |
|---|---|
| **Chart type** | Bar chart (horizontal) |
| **Nombre del componente** | `Top 10 Países con Mayor Error` |
| **Custom Query** | KPI 5 de `sql/kpis.sql` |
| **Dimension** | `country_name` |
| **Metric** | `mae` (SUM, ya viene agregado) |
| **Sort** | `mae` descendente |
| **Rows per page** | 10 (fijo) |

**Estilo:**
- Bars color `#D32F2F` con gradiente al `#F57C00` (Conditional color formatting por valor).
- Data labels: 3 decimales.
- Title del eje X: `MAE absoluto`.
- Ocultar leyenda.

**Lectura esperada:** los 10 países en orden descendente de error. Comúnmente outliers regionales (regiones con `Region=Unknown` después del backfill).

---

## KPI 6 — `Distribución de Errores` (Column chart / histograma)

**Propósito:** verificar que los residuos del modelo son aproximadamente simétricos en torno a 0 (supuesto de regresión lineal).

**Configuración en Looker Studio:**

| Campo | Valor |
|---|---|
| **Chart type** | Column chart |
| **Nombre del componente** | `Distribución de Errores` |
| **Custom Query** | KPI 6 de `sql/kpis.sql` |
| **Dimension** | `error_bucket` (rango: `-2.0` a `+2.0` aprox.) |
| **Metric** | `freq` (SUM) |
| **Sort** | `error_bucket` ascendente |

**Estilo:**
- Color de columnas: `#1976D2`.
- Mostrar **Reference line** vertical en `x = 0` color `#D32F2F`, grosor 2 px, label `error = 0`.
- Y-axis: `Frecuencia`. X-axis: `Error (predicho − actual)`.
- Activar data labels solo si hay ≤ 30 buckets.

**Lectura esperada:** campana centrada en 0 con colas simétricas; si está sesgada hacia un lado, el modelo sobre/subestima sistemáticamente.

---

## KPI 7 — `Estado de Procesamiento` (Pie chart)

**Propósito:** mostrar el reparto entre eventos exitosos y las distintas categorías de fallo (validación del producer con ruido al 5 %).

**Configuración en Looker Studio:**

| Campo | Valor |
|---|---|
| **Chart type** | Pie chart |
| **Nombre del componente** | `Estado de Procesamiento` |
| **Custom Query** | KPI 7 de `sql/kpis.sql` |
| **Dimension** | `processing_status` |
| **Metric** | `n` (SUM) |
| **Sort** | `n` descendente |

**Estilo (colores por slice — Conditional dimension color):**
- `VALID` → `#388E3C`.
- `INVALID_SCHEMA` → `#616161`.
- `INVALID_VALUES` → `#F57C00`.
- `PREDICTION_ERROR` → `#7B1FA2`.
- Donut hole: 50 %.
- Mostrar porcentaje + valor absoluto en las etiquetas (Show data labels → Percentage and value).
- Leyenda a la derecha.

**Lectura esperada:** ~95 % VALID en modo con ruido, 100 % VALID en modo `--clean`.

---

## KPI 8 — `Throughput por Minuto` (Time series + área)

**Propósito:** medir la velocidad efectiva del consumer (eventos procesados por minuto) y detectar caídas.

**Configuración en Looker Studio:**

| Campo | Valor |
|---|---|
| **Chart type** | Time series (area) |
| **Nombre del componente** | `Throughput por Minuto` |
| **Custom Query** | KPI 8 de `sql/kpis.sql` |
| **Time Dimension** | `minute_bucket` (tipo Date Hour Minute) |
| **Metric** | `events` (SUM) |
| **Sort** | `minute_bucket` ascendente |

**Estilo:**
- Area color `#1976D2` con opacidad 30 %, línea superior sólida 2 px.
- Y-axis: `Eventos/minuto`. X-axis: ocultar título.
- Activar **Reference line horizontal** en el promedio (formula: `AVG(events)`) color `#388E3C`, label `Media`.

**Lectura esperada:** con `PRODUCER_DELAY_SECONDS=0.5` esperar ~120 eventos/minuto. Si baja drásticamente, el consumer o MySQL están saturados.

---

## Layout final recomendado

Tablero de **2 columnas × 4 filas** (1240 × 1600 px aprox.):

```
+---------------------------+---------------------------+
| KPI 1: MAE Global         | KPI 7: Estado Procesamto.|
| (Scorecard)               | (Pie chart)               |
+---------------------------+---------------------------+
| KPI 3: Predicho vs Actual | KPI 6: Distribución Err.  |
| (Scatter)                 | (Column chart)            |
+---------------------------+---------------------------+
| KPI 4: Tendencia Diaria   | KPI 8: Throughput/min     |
| (Time series)             | (Time series area)        |
+---------------------------+---------------------------+
| KPI 2: Predicciones País  | KPI 5: Top 10 Error       |
| (Bar chart)               | (Bar chart)               |
+---------------------------+---------------------------+
```

### Header del dashboard
- **Texto principal:** `Streaming Happiness Predictions Dashboard`
- **Subtítulo:** `World Happiness Report 2015–2019 · Kafka + MySQL + Linear Regression`
- **Footer:** `Workshop-3 — ETL G01 — 2026-1 — UAO`
- **Logo:** insertar `dashboards/screenshots/uao_logo.png` en la esquina superior derecha (si está disponible).

### Filtros globales (opcional pero recomendado)
Añadir en la parte superior un **Control → Date range** que filtre `prediction_timestamp` y un **Control → Drop-down list** que filtre `country_name`. Eso permite explorar el dashboard sin tocar SQL.

---

## Checklist de verificación final

Antes de tomar el screenshot final que va a `dashboards/screenshots/dashboard_complete.png`:

- [ ] Los 8 charts tienen el **nombre exacto** indicado en este documento.
- [ ] Las queries de los 8 charts coinciden literalmente con los bloques de `sql/kpis.sql` (sin punto y coma final si Looker se queja).
- [ ] La fuente de datos no muestra `Configuration incomplete` en ningún chart.
- [ ] El número del Scorecard (KPI 1) coincide con la consulta directa en MySQL: `SELECT AVG(ABS(prediction_error)) FROM fact_predictions;`.
- [ ] El total de la pie (KPI 7) suma `COUNT(*) FROM raw_happiness_events`.
- [ ] `COUNT(fact_predictions) == COUNT(VALID en raw_happiness_events)` — invariante crítica del pipeline.
- [ ] Capturar 1 screenshot global + 1 por chart en `dashboards/screenshots/`.
