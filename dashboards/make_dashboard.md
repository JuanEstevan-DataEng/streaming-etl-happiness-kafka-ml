# Complete Guide: Dashboard in Looker Studio (Google Data Studio)

This document covers **the full flow** required to build the workshop dashboard: from exposing MySQL through ngrok, creating the Looker user, connecting the data source, all the way to a step-by-step recipe for each of the 8 charts including exact name, query, dimensions, metrics and styling.

---

## Prerequisites

- Infrastructure up and running via `docker compose up -d` (Zookeeper, Kafka and MySQL containers).
- MySQL schema automatically initialized from `sql/create_tables.sql` (happens on first container boot).
- ngrok installed and authenticated.
- Google account for Looker Studio.
- At least one complete streaming run (`python kafka/consumer.py` + `python kafka/producer.py`) so the tables contain data.
- Keep `sql/kpis.sql` open in parallel to copy the 8 queries.

---

# PART A — Data source setup

## Step 1 — Expose MySQL via ngrok

1. In a new terminal: `ngrok tcp 3307` (the MySQL container publishes host port 3307 → container port 3306).
2. Copy the `host:port` returned by ngrok (e.g. `0.tcp.ngrok.io:14523`).

## Step 2 — Create a read-only user for Looker

SQL to execute:

```sql
CREATE USER IF NOT EXISTS 'looker'@'%' IDENTIFIED BY 'looker_pwd_changeme';
GRANT SELECT ON happiness.* TO 'looker'@'%';
FLUSH PRIVILEGES;
```

**Option A — single command (recommended):** run it directly without an intermediate file. The password is hardcoded and must match the one in `.env` (default `changeme_root`):

```bash
docker exec -i ws3_mysql mysql -uroot -pchangeme_root happiness <<'SQL'
CREATE USER IF NOT EXISTS 'looker'@'%' IDENTIFIED BY 'looker_pwd_changeme';
GRANT SELECT ON happiness.* TO 'looker'@'%';
FLUSH PRIVILEGES;
SQL
```

> ⚠️ **If you get `ERROR 1045 (28000): Access denied for user 'root'@'localhost' (using password: YES)`**, it is because `$MYSQL_ROOT_PASSWORD` is not exported in the host shell (only `docker compose` reads it from `.env`). To avoid this, either hardcode the password as above, or load `.env` into the current session first:
>
> ```bash
> set -a && source .env && set +a
> docker exec -i ws3_mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" happiness <<'SQL'
> CREATE USER IF NOT EXISTS 'looker'@'%' IDENTIFIED BY 'looker_pwd_changeme';
> GRANT SELECT ON happiness.* TO 'looker'@'%';
> FLUSH PRIVILEGES;
> SQL
> ```

**Option B — using a SQL file:** first create `grant_looker.sql` with the SQL block above and then run:

```bash
docker exec -i ws3_mysql mysql -uroot -pchangeme_root happiness < grant_looker.sql
```

**Verification** that the user was created and can read:

```bash
docker exec -i ws3_mysql mysql -ulooker -plooker_pwd_changeme happiness \
  -e "SELECT COUNT(*) AS rows_in_fact FROM fact_predictions;"
```

It should return a single row with the number of predictions loaded. If the query responds correctly, the credentials are ready to be used in Looker Studio.

## Step 3 — Connect Looker Studio to MySQL

1. Open `lookerstudio.google.com` → **Create** → **Data source** → **MySQL**.
2. Fill in:
   - **Host:** `0.tcp.ngrok.io`
   - **Port:** the one returned by ngrok (e.g. `14523`)
   - **Database:** `happiness`
   - **User:** `looker`
   - **Password:** `looker_pwd_changeme`
3. Click **Authenticate** → **Connect**.
4. Rename the data source to `happiness@ngrok` so it can be referenced from each chart.

---

# PART B — Building the 8 charts

## General convention for all 8 charts

For **every** KPI, the Looker Studio flow is:

1. **Insert → Chart** and pick the chart type listed in the table.
2. In the **DATA** panel (right side), click the current data source → **+ Add data source** (if not already connected) → or select `happiness@ngrok`.
3. When selecting the table, tick the **CUSTOM QUERY** option and paste the matching query from `sql/kpis.sql`.
4. Click **ADD** → Looker will show the detected fields.
5. Configure **Dimension** and **Metric** as indicated in each section.
6. Switch to the **STYLE** panel and apply the styling recommendations.
7. Rename the chart by double-clicking its header → type the **exact name** listed below.

> **Recommended global palette:**
> - Error red: `#D32F2F`
> - Success green / VALID: `#388E3C`
> - Warning orange / INVALID_VALUES: `#F57C00`
> - Neutral grey / INVALID_SCHEMA: `#616161`
> - Prediction purple / PREDICTION_ERROR: `#7B1FA2`
> - Reference blue: `#1976D2`

---

## KPI 1 — `Global MAE` (Scorecard)

**Purpose:** show at a glance how far the predictions are from the actual values, averaged across all VALID events.

**Looker Studio configuration:**

| Field | Value |
|---|---|
| **Chart type** | Scorecard |
| **Component name** | `Global MAE` |
| **Custom Query** | `SELECT AVG(ABS(prediction_error)) AS mae_global FROM fact_predictions;` |
| **Metric** | `mae_global` (Number type, aggregation `Auto` or `Average` — the query already aggregates) |
| **Dimension** | _(none)_ |
| **Comparison metric** | disabled |

**Style:**
- Decimal places: `3`.
- Number format: `Number`.
- Label: `MAE`.
- Number color: `#D32F2F` if value > 0.5, `#388E3C` if ≤ 0.5 (use **Conditional formatting** → "Single color" → rule `mae_global > 0.5`).
- Background: white. Border: 1 px grey.

**Expected read:** a single number around `0.4–0.5` (the offline value produced by the trained model).

---

## KPI 2 — `Predictions by Country` (Horizontal bar chart)

**Purpose:** see which countries have received the most successfully processed events.

**Looker Studio configuration:**

| Field | Value |
|---|---|
| **Chart type** | Bar chart (horizontal) |
| **Component name** | `Predictions by Country` |
| **Custom Query** | KPI 2 from `sql/kpis.sql` |
| **Dimension** | `country_name` |
| **Metric** | `n_predictions` (SUM) |
| **Sort** | `n_predictions` descending |
| **Rows per page** | 20 |

**Style:**
- Single color bars: `#1976D2`.
- Show data labels at the end of each bar.
- Y-axis label: `Country`. X-axis label: `# VALID events`.
- X-axis with light grey grid lines.

**Expected read:** countries that appear in more years (up to 5: 2015–2019) show up first.

---

## KPI 3 — `Predicted vs Actual` (Scatter plot)

**Purpose:** visually verify that the model does not diverge from the actual value; ideally the points fall on the `y = x` diagonal.

**Looker Studio configuration:**

| Field | Value |
|---|---|
| **Chart type** | Scatter chart |
| **Component name** | `Predicted vs Actual` |
| **Custom Query** | KPI 3 from `sql/kpis.sql` |
| **Dimension** | _(none; each row is a point)_ |
| **Metric X** | `actual_score` |
| **Metric Y** | `predicted_score` |
| **Bubble size** | _(none)_ |

**Style:**
- Point shape: circle, size 6 px.
- Color: `#388E3C` at 50% opacity.
- Enable **Trendline** → Linear → color `#D32F2F`, 2 px thick.
- Both axes with the same limits: min `0`, max `10`, interval `1`.
- Enable **Show axis title** on both axes (`Actual happiness`, `Predicted happiness`).

**Expected read:** a 45° point cloud with symmetric dispersion; the trendline should sit almost exactly on the diagonal.

---

## KPI 4 — `Daily Prediction Trend` (Time series)

> ⚠️ **Not implemented in the final dashboard.** The pipeline was only executed once, so every record in `fact_predictions` shares essentially the same `prediction_timestamp`. The resulting chart would collapse into a single point and provide no useful information. The query is kept in `sql/kpis.sql` for future runs with multi-day data.

**Purpose:** show that the pipeline runs over time and compare the average predicted vs. the average actual score day by day.

**Looker Studio configuration (for reference):**

| Field | Value |
|---|---|
| **Chart type** | Time series |
| **Component name** | `Daily Prediction Trend` |
| **Custom Query** | KPI 4 from `sql/kpis.sql` |
| **Time Dimension** | `day` (Date type) |
| **Metrics** | `avg_predicted`, `avg_actual` (two series) |
| **Sort** | `day` ascending |

**Style:**
- `avg_predicted` series: solid line, color `#1976D2`, 2 px thick, points visible.
- `avg_actual` series: dashed line, color `#388E3C`, 2 px thick.
- Show legend at the bottom.
- Y-axis: min `0`, max `10`.

**Expected read:** both lines should track each other very closely; large gaps on any given day point to batches dominated by atypical countries.

> **Note:** if all streaming happens on the same day, this chart collapses to a single point. In that case, replace `DATE()` with `DATE_FORMAT(prediction_timestamp, '%Y-%m-%d %H:%i')` in `sql/kpis.sql` for per-minute granularity and re-import the query.

---

## KPI 5 — `Top 10 Countries by Error` (Horizontal bar chart)

**Purpose:** identify the countries where the model makes the largest average mistake (a starting point to iterate features or mappings).

**Looker Studio configuration:**

| Field | Value |
|---|---|
| **Chart type** | Bar chart (horizontal) |
| **Component name** | `Top 10 Countries by Error` |
| **Custom Query** | KPI 5 from `sql/kpis.sql` |
| **Dimension** | `country_name` |
| **Metric** | `mae` (SUM, already aggregated) |
| **Sort** | `mae` descending |
| **Rows per page** | 10 (fixed) |

**Style:**
- Bars colored `#D32F2F` with a gradient toward `#F57C00` (Conditional color formatting by value).
- Data labels: 3 decimals.
- X-axis title: `Absolute MAE`.
- Hide legend.

**Expected read:** the 10 countries sorted by error in descending order. Often regional outliers (rows where `Region=Unknown` after backfill).

---

## KPI 6 — `Error Distribution` (Column chart / histogram)

**Purpose:** verify that the model residuals are roughly symmetric around 0 (a linear regression assumption).

**Looker Studio configuration:**

| Field | Value |
|---|---|
| **Chart type** | Column chart |
| **Component name** | `Error Distribution` |
| **Custom Query** | KPI 6 from `sql/kpis.sql` |
| **Dimension** | `error_bucket` (range: approximately `-2.0` to `+2.0`) |
| **Metric** | `freq` (SUM) |
| **Sort** | `error_bucket` ascending |

**Style:**
- Column color: `#1976D2`.
- Show a vertical **Reference line** at `x = 0`, color `#D32F2F`, 2 px thick, label `error = 0`.
- Y-axis: `Frequency`. X-axis: `Error (predicted − actual)`.
- Enable data labels only when there are ≤ 30 buckets.

**Expected read:** a bell shape centered at 0 with symmetric tails; if it is skewed, the model systematically over/underestimates.

---

## KPI 7 — `Processing Status` (Pie chart)

**Purpose:** show the split between successful events and the different failure categories (producer validation with 5% noise injection).

**Looker Studio configuration:**

| Field | Value |
|---|---|
| **Chart type** | Pie chart |
| **Component name** | `Processing Status` |
| **Custom Query** | KPI 7 from `sql/kpis.sql` |
| **Dimension** | `processing_status` |
| **Metric** | `n` (SUM) |
| **Sort** | `n` descending |

**Style (per-slice colors — Conditional dimension color):**
- `VALID` → `#388E3C`.
- `INVALID_SCHEMA` → `#616161`.
- `INVALID_VALUES` → `#F57C00`.
- `PREDICTION_ERROR` → `#7B1FA2`.
- Donut hole: 50%.
- Show percentage + absolute value on labels (Show data labels → Percentage and value).
- Legend on the right.

**Expected read:** ~95% VALID under noisy mode, 100% VALID under `--clean`.

---

## KPI 8 — `Throughput per Minute` (Area time series)

**Purpose:** measure the consumer's effective speed (events processed per minute) and detect dips.

**Looker Studio configuration:**

| Field | Value |
|---|---|
| **Chart type** | Time series (area) |
| **Component name** | `Throughput per Minute` |
| **Custom Query** | KPI 8 from `sql/kpis.sql` |
| **Time Dimension** | `minute_bucket` (Date Hour Minute type) |
| **Metric** | `events` (SUM) |
| **Sort** | `minute_bucket` ascending |

**Style:**
- Area color `#1976D2` at 30% opacity, solid top line 2 px thick.
- Y-axis: `Events/minute`. X-axis: hide title.
- Add a horizontal **Reference line** at the average (formula: `AVG(events)`), color `#388E3C`, label `Mean`.

**Expected read:** with `PRODUCER_DELAY_SECONDS=0.5` expect ~120 events per minute. If it drops sharply, the consumer or MySQL are saturated.

---

## Recommended final layout

A **2-column × 4-row** board (≈ 1240 × 1600 px):

```
+---------------------------+---------------------------+
| KPI 1: Global MAE         | KPI 7: Processing Status  |
| (Scorecard)               | (Pie chart)               |
+---------------------------+---------------------------+
| KPI 3: Predicted vs Actual| KPI 6: Error Distribution |
| (Scatter)                 | (Column chart)            |
+---------------------------+---------------------------+
| KPI 4: Daily Trend        | KPI 8: Throughput/min     |
| (Time series — skipped)   | (Time series area)        |
+---------------------------+---------------------------+
| KPI 2: Predictions/Country| KPI 5: Top 10 Error       |
| (Bar chart)               | (Bar chart)               |
+---------------------------+---------------------------+
```

### Dashboard header
- **Main text:** `Happiness in Motion: Real-Time ML Predictions over a Kafka Pipeline`
- **Subtitle:** `World Happiness Report 2015–2019 · Kafka + MySQL + Linear Regression`
- **Footer:** `Workshop-3 — ETL G01 — 2026-1 — UAO`
- **Logo:** insert `dashboards/screenshots/uao_logo.png` in the top-right corner (if available).

### Global filters (optional but recommended)
Add at the top a **Control → Date range** filtering `prediction_timestamp` and a **Control → Drop-down list** filtering `country_name`. This lets viewers explore the dashboard without touching SQL.

---

## Final verification checklist

Before taking the final screenshot that goes to `dashboards/screenshots/dashboard_complete.png`:

- [ ] The 8 charts have the **exact name** listed in this document.
- [ ] The 8 chart queries match the blocks in `sql/kpis.sql` literally (drop the trailing semicolon if Looker complains).
- [ ] No chart shows `Configuration incomplete` for its data source.
- [ ] The Scorecard (KPI 1) value matches the direct MySQL query: `SELECT AVG(ABS(prediction_error)) FROM fact_predictions;`.
- [ ] The pie total (KPI 7) equals `COUNT(*) FROM raw_happiness_events`.
- [ ] `COUNT(fact_predictions) == COUNT(VALID in raw_happiness_events)` — critical pipeline invariant.
- [ ] Capture 1 global screenshot plus 1 per chart into `dashboards/screenshots/`.
