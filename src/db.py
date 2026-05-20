"""SQLAlchemy engine factory and CRUD helpers for the streaming pipeline."""

import json
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from src.config import MYSQL_URL

_engine: Optional[Engine] = None

def get_engine() -> Engine:
    """Return a singleton SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(MYSQL_URL, pool_pre_ping=True, future=True)
    return _engine

def insert_raw_event(payload_text: str, offset: int, partition: int,
                     status: str, error_detail: Optional[str] = None) -> int:
    """Insert a raw event row and return its raw_event_id."""
    # Wrap non-JSON strings so MySQL never crashes on the JSON cast
    try:
        json.loads(payload_text)
        payload_for_db = payload_text
    except json.JSONDecodeError:
        payload_for_db = json.dumps({"_raw": payload_text})
    sql = text("""
        INSERT INTO raw_happiness_events
          (kafka_offset, kafka_partition, raw_payload, processing_status, error_detail)
        VALUES (:o, :p, CAST(:payload AS JSON), :s, :e)
    """)
    with get_engine().begin() as conn:
        result = conn.execute(sql, {"o": offset, "p": partition,
                                     "payload": payload_for_db,
                                     "s": status, "e": error_detail})
        return result.lastrowid

def update_raw_status(raw_event_id: int, status: str,
                      error_detail: Optional[str] = None) -> None:
    """Update processing_status of a previously inserted raw event."""
    sql = text("""UPDATE raw_happiness_events
                  SET processing_status = :s, error_detail = :e
                  WHERE raw_event_id = :id""")
    with get_engine().begin() as conn:
        conn.execute(sql, {"s": status, "e": error_detail, "id": raw_event_id})

def upsert_country(country_name: str, region: Optional[str] = None) -> int:
    """Insert or fetch a country_id by name."""
    with get_engine().begin() as conn:
        conn.execute(text("""INSERT IGNORE INTO dim_country (country_name, region)
                              VALUES (:n, :r)"""), {"n": country_name, "r": region})
        return conn.execute(text("""SELECT country_id FROM dim_country
                                     WHERE country_name = :n"""),
                            {"n": country_name}).scalar_one()

def upsert_date(year: int) -> int:
    """Insert or fetch a date_id by year."""
    with get_engine().begin() as conn:
        conn.execute(text("INSERT IGNORE INTO dim_date (year) VALUES (:y)"), {"y": year})
        return conn.execute(text("SELECT date_id FROM dim_date WHERE year = :y"),
                            {"y": year}).scalar_one()

def upsert_dim_raw_event(raw_event_id: int, country_name: str, year: int) -> None:
    """Insert into dim_raw_event for joins; idempotent via INSERT IGNORE on PK."""
    with get_engine().begin() as conn:
        conn.execute(text("""INSERT IGNORE INTO dim_raw_event
                              (raw_event_id, country_name, year)
                              VALUES (:id, :c, :y)"""),
                     {"id": raw_event_id, "c": country_name, "y": year})

def insert_prediction(raw_event_id: int, country_id: int, date_id: int,
                      actual: Optional[float], predicted: float,
                      error: Optional[float]) -> int:
    """Insert a row in fact_predictions and return prediction_id."""
    sql = text("""INSERT INTO fact_predictions
                   (raw_event_id, country_id, date_id,
                    actual_score, predicted_score, prediction_error)
                   VALUES (:r, :c, :d, :a, :p, :e)""")
    with get_engine().begin() as conn:
        result = conn.execute(sql, {"r": raw_event_id, "c": country_id, "d": date_id,
                                     "a": actual, "p": predicted, "e": error})
        return result.lastrowid

def truncate_tables() -> None:
    """Clear all records from the database for a fresh demonstration."""
    with get_engine().begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.execute(text("TRUNCATE TABLE fact_predictions;"))
        conn.execute(text("TRUNCATE TABLE dim_raw_event;"))
        conn.execute(text("TRUNCATE TABLE raw_happiness_events;"))
        conn.execute(text("TRUNCATE TABLE dim_country;"))
        conn.execute(text("TRUNCATE TABLE dim_date;"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
