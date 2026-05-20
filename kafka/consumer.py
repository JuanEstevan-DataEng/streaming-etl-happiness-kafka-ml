"""Kafka consumer: persist raw event, validate, predict, store fact row."""

import json
import sys
from pathlib import Path

import joblib
import numpy as np
from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC
from src.paths import MODELS_DIR
from src.db import (insert_raw_event, update_raw_status,
                    upsert_country, upsert_date, upsert_dim_raw_event,
                    insert_prediction, truncate_tables)
from src.schema import validate_event, MODEL_FEATURE_ORDER

def load_model():
    """Load the serialized sklearn Pipeline once at startup."""
    model_path = MODELS_DIR / "model.pkl"
    if not model_path.exists():
        sys.exit(f"FAIL: {model_path} no existe. Ejecuta Fase A primero.")
    return joblib.load(model_path)

def process_message(msg, model) -> None:
    """Process a single Kafka message end-to-end."""

    raw_text = msg.value.decode("utf-8", errors="replace")

    # Try to parse JSON. If it fails, persist raw and mark INVALID_SCHEMA.
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as e:
        insert_raw_event(raw_text, msg.offset, msg.partition,
                         "INVALID_SCHEMA", f"JSON inválido: {e}")
        print(f"[offset={msg.offset}] status=INVALID_SCHEMA (JSON malformado)")
        return

    # Persist the raw event with a provisional status, then validate
    raw_event_id = insert_raw_event(raw_text, msg.offset, msg.partition, "VALID")

    status, detail = validate_event(payload)
    if status != "VALID":
        update_raw_status(raw_event_id, status, detail)
        print(f"[offset={msg.offset}] status={status} detail={detail}")
        return

    # Predict and persist into the star schema
    try:
        country_id = upsert_country(payload["country"], region=None)
        date_id = upsert_date(int(payload["year"]))
        upsert_dim_raw_event(raw_event_id, payload["country"], int(payload["year"]))

        # Strict feature ordering matches training
        features = np.array([[payload[f] for f in MODEL_FEATURE_ORDER]], dtype=float)
        predicted = float(model.predict(features)[0])
        actual = float(payload["actual_happiness_score"])
        error = predicted - actual

        insert_prediction(raw_event_id, country_id, date_id, actual, predicted, error)
        print(f"[offset={msg.offset}] status=VALID country={payload['country']} "
              f"predicted={predicted:.3f} actual={actual:.3f}")
    except Exception as e:
        update_raw_status(raw_event_id, "PREDICTION_ERROR", str(e))
        print(f"[offset={msg.offset}] status=PREDICTION_ERROR detail={e}")

def main() -> None:
    """Entry point: consume the topic in a blocking loop."""
    print("Clearing database tables for a fresh demonstration...")
    truncate_tables()

    model = load_model()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="happiness-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        # Keep raw bytes so we can store malformed JSON in the raw table
        value_deserializer=lambda b: b,
    )
    print(f"Listening on topic={KAFKA_TOPIC} ...")
    try:
        for msg in consumer:
            process_message(msg, model)
    except KeyboardInterrupt:
        print("Interrupted by user, closing consumer ...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
