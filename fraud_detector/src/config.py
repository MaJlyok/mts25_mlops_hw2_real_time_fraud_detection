import json
import logging
import os
from pathlib import Path

# Корень сервиса (папка fraud_detector): .../fraud_detector/src/config.py -> parents[1]
BASE_DIR = Path(__file__).resolve().parents[1]

# --- Kafka 
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions") 
PROCESSED_TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC", "transactions_processed") 
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring") 


BATCH_SIZE = 1000
BATCH_TIMEOUT_SEC = 2.0

# --- Артефакты модели 
ARTIFACTS_DIR = BASE_DIR / "artifacts"
MODEL_PATH = ARTIFACTS_DIR / "model.cbm"
META_PATH = ARTIFACTS_DIR / "meta.json"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


def setup_logging(name):
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    return logging.getLogger(name)


def load_meta():
    with open(META_PATH, encoding="utf-8") as f:
        return json.load(f)
