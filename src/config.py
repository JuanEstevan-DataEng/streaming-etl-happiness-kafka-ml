"""Environment configuration loader. Reads .env once on import."""
# ES: Cargador de configuración desde .env. Lee una sola vez al importar.

import os
from dotenv import load_dotenv
from src.paths import ROOT

# Load .env from project root
# ES: Carga el archivo .env desde la raíz del proyecto
load_dotenv(ROOT / ".env")

# MySQL settings
# ES: Configuración de MySQL
MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "changeme_root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "happiness")
MYSQL_USER = os.getenv("MYSQL_USER", "happiness_app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "changeme_app")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

# SQLAlchemy URL used by src/db.py
# ES: URL de SQLAlchemy usada por src/db.py
MYSQL_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8mb4"
)

# Kafka settings
# ES: Configuración de Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "happiness-predictions")

# Producer pacing and noise injection
# ES: Cadencia y ruido del producer
PRODUCER_DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.5"))
PRODUCER_NOISE_RATIO = float(os.getenv("PRODUCER_NOISE_RATIO", "0.05"))
