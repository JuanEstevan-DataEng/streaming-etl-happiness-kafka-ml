-- KPI queries consumed by Looker Studio. Each block = one chart.

-- ===== KPI 1: MAE global (scorecard) =====
SELECT AVG(ABS(prediction_error)) AS mae_global
FROM fact_predictions;

-- ===== KPI 2: Predicciones por país (bar chart) =====
SELECT c.country_name, COUNT(*) AS n_predictions
FROM fact_predictions p
JOIN dim_country c ON c.country_id = p.country_id
GROUP BY c.country_name
ORDER BY n_predictions DESC;

-- ===== KPI 3: Predicho vs Actual (scatter) =====
SELECT actual_score, predicted_score
FROM fact_predictions;

-- ===== KPI 4: Tendencia temporal (time series) =====
SELECT DATE(prediction_timestamp) AS day,
       AVG(predicted_score)       AS avg_predicted,
       AVG(actual_score)          AS avg_actual
FROM fact_predictions
GROUP BY day
ORDER BY day;

-- ===== KPI 5: Top-10 países por error absoluto medio (bar chart) =====
SELECT c.country_name, AVG(ABS(p.prediction_error)) AS mae
FROM fact_predictions p
JOIN dim_country c ON c.country_id = p.country_id
GROUP BY c.country_name
ORDER BY mae DESC
LIMIT 10;

-- ===== KPI 6: Distribución de errores (histograma) =====
SELECT ROUND(prediction_error, 1) AS error_bucket, COUNT(*) AS freq
FROM fact_predictions
GROUP BY error_bucket
ORDER BY error_bucket;

-- ===== KPI 7: Breakdown de estados de procesamiento (pie chart) =====
SELECT processing_status, COUNT(*) AS n
FROM raw_happiness_events
GROUP BY processing_status;

-- ===== KPI 8: Throughput (eventos por minuto) =====
SELECT DATE_FORMAT(received_at, '%Y-%m-%d %H:%i:00') AS minute_bucket,
       COUNT(*) AS events
FROM raw_happiness_events
GROUP BY minute_bucket
ORDER BY minute_bucket;
