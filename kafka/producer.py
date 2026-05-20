"""Kafka producer that streams test.csv rows as JSON events."""
# ES: Producer de Kafka que streamea filas de test.csv como eventos JSON

import argparse
import json
import random
import sys
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import (KAFKA_BOOTSTRAP, KAFKA_TOPIC,
                        PRODUCER_DELAY_SECONDS, PRODUCER_NOISE_RATIO)
from src.paths import DATA_PROCESSED

# Output JSON field order matches what the consumer/validator expects
# ES: El orden de campos coincide con lo que espera el consumer/validador
OUTPUT_FIELDS = ["country", "year", "gdp", "family", "health",
                 "freedom", "generosity", "corruption", "actual_happiness_score"]

def build_event(row: pd.Series) -> dict:
    """Build a JSON-ready event from a test.csv row."""
    # ES: Construye un evento listo para JSON a partir de una fila de test.csv
    return {
        "country": str(row["country"]),
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row["happiness_score"]),
    }

def corrupt_event(event: dict) -> dict:
    """Randomly damage an event to trigger consumer validation paths."""
    # ES: Daña un evento al azar para probar las rutas de validación del consumer
    mode = random.choice(["drop_field", "type_swap", "out_of_range"])
    bad = dict(event)
    if mode == "drop_field":
        bad.pop(random.choice(["gdp", "family", "health"]))
    elif mode == "type_swap":
        bad["gdp"] = "not_a_number"
    elif mode == "out_of_range":
        bad["family"] = 999.0
    return bad

def main(clean: bool) -> None:
    """Main loop: read test.csv and emit events."""
    # ES: Loop principal: lee test.csv y emite eventos
    test_path = DATA_PROCESSED / "test.csv"
    if not test_path.exists():
        sys.exit(f"FAIL: {test_path} no existe. Ejecuta Fase A primero.")
    df = pd.read_csv(test_path)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    total = len(df)
    for i, raw_row in enumerate(df.to_dict(orient="records"), start=1):
        event = build_event(pd.Series(raw_row))
        # Inject noise occasionally to exercise consumer validation
        # ES: Inyecta ruido ocasionalmente para ejercitar la validación del consumer
        if not clean and random.random() < PRODUCER_NOISE_RATIO:
            event = corrupt_event(event)
        producer.send(KAFKA_TOPIC, value=event)
        print(f"[{i}/{total}] sent country={event.get('country','?')} "
              f"year={event.get('year','?')}")
        time.sleep(PRODUCER_DELAY_SECONDS)

    producer.flush()
    producer.close()
    print(f"DONE: {total} eventos enviados al topic {KAFKA_TOPIC}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean", action="store_true",
                        help="Disable noise injection (send only valid events)")
    args = parser.parse_args()
    main(clean=args.clean)
