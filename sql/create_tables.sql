-- MySQL schema initialization for Workshop-3

CREATE DATABASE IF NOT EXISTS happiness
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE happiness;

-- Raw events table: stores every Kafka message as-is, including invalid ones
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    kafka_offset      BIGINT,
    kafka_partition   INT,
    raw_payload       JSON NOT NULL,
    processing_status ENUM('VALID','INVALID_SCHEMA','INVALID_VALUES','PREDICTION_ERROR') NOT NULL,
    error_detail      TEXT,
    received_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_status (processing_status),
    INDEX idx_received_at (received_at)
);

-- Country dimension
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(120) NOT NULL UNIQUE,
    region       VARCHAR(80)
);

-- Date dimension (yearly granularity matches the dataset)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    year    INT NOT NULL UNIQUE
);

-- Raw-event dimension: 1-to-1 with raw_happiness_events, denormalized for joins
CREATE TABLE IF NOT EXISTS dim_raw_event (
    raw_event_id BIGINT PRIMARY KEY,
    country_name VARCHAR(120),
    year         INT,
    FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events(raw_event_id)
);

-- Fact table: one row per successful prediction
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    raw_event_id         BIGINT NOT NULL,
    country_id           INT NOT NULL,
    date_id              INT NOT NULL,
    actual_score         DOUBLE,
    predicted_score      DOUBLE NOT NULL,
    prediction_error     DOUBLE,
    prediction_timestamp DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events(raw_event_id),
    FOREIGN KEY (country_id)   REFERENCES dim_country(country_id),
    FOREIGN KEY (date_id)      REFERENCES dim_date(date_id),
    INDEX idx_prediction_ts (prediction_timestamp)
);
