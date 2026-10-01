import json
import logging
import os

import psycopg2
from confluent_kafka import Consumer
from psycopg2.extras import execute_values

logging.basicConfig(level="INFO", format="%(asctime)s [writer] %(levelname)s: %(message)s")
logger = logging.getLogger("writer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
BATCH_SIZE = 1000
BATCH_TIMEOUT_SEC = 2.0

INSERT_SQL = """
    INSERT INTO scores (transaction_id, score, fraud_flag)
    VALUES %s
    ON CONFLICT (transaction_id) DO NOTHING
"""


def parse_messages(raw_values):

    rows = []
    for raw in raw_values:
        try:
            msg = json.loads(raw.decode("utf-8"))
            rows.append((str(msg["transaction_id"]), float(msg["score"]), int(msg["fraud_flag"])))
        except Exception as e: 
            logger.warning("Пропускаем битое сообщение: %s", e)
    return rows


def main():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "fraud"),
        user=os.getenv("POSTGRES_USER", "fraud"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud"),
    )
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "score_writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([SCORING_TOPIC])
    logger.info("Старт: %s -> Postgres (таблица scores)", SCORING_TOPIC)

    while True:
        messages = consumer.consume(BATCH_SIZE, BATCH_TIMEOUT_SEC)
        if not messages:
            continue

        values = []
        for m in messages:
            if m.error():
                logger.error("Ошибка Kafka: %s", m.error())
            else:
                values.append(m.value())

        rows = parse_messages(values)
        with conn, conn.cursor() as cur: 
            execute_values(cur, INSERT_SQL, rows)
        consumer.commit(asynchronous=False)
        logger.info("Батч: прочитано %d, записано %d", len(messages), len(rows))


if __name__ == "__main__":
    main()
