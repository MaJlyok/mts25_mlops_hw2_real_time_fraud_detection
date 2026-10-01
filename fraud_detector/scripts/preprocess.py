import json
import sys
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import config  
from features import RAW_REQUIRED_COLUMNS, build_features  

logger = config.setup_logging("preprocess")

def parse_messages(raw_values):
    ids, rows = [], []
    for raw in raw_values:
        try:
            msg = json.loads(raw.decode("utf-8"))
            transaction_id = msg["transaction_id"]
            data = msg["data"]
            missing = [c for c in RAW_REQUIRED_COLUMNS if c not in data]
            if missing:
                raise ValueError(f"нет полей {missing}")
        except Exception as e: 
            logger.warning("Пропускаем битое сообщение: %s", e)
            continue
        ids.append(transaction_id)
        rows.append(data)
    return ids, rows


def make_output_messages(ids, rows, meta):
    if not rows:
        return []
    features = build_features(pd.DataFrame(rows), meta["impute_values"])
    records = features.to_dict(orient="records")
    return [
        {"transaction_id": tid, "features": feats}
        for tid, feats in zip(ids, records)
    ]


def main():
    meta = config.load_meta()

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "preprocess",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([config.TRANSACTIONS_TOPIC])

    logger.info(
        "Старт: %s -> %s, batch=%d",
        config.TRANSACTIONS_TOPIC, config.PROCESSED_TOPIC, config.BATCH_SIZE,
    )
    while True:
        messages = consumer.consume(config.BATCH_SIZE, config.BATCH_TIMEOUT_SEC)
        if not messages:
            continue

        valid = []
        for m in messages:
            if m.error():
                logger.error("Ошибка Kafka: %s", m.error())
            else:
                valid.append(m.value())

        ids, rows = parse_messages(valid)
        out = make_output_messages(ids, rows, meta)

        for item in out:
            producer.produce(
                config.PROCESSED_TOPIC,
                key=str(item["transaction_id"]),
                value=json.dumps(item).encode("utf-8"),
            )
        producer.flush()
        consumer.commit(asynchronous=False)
        logger.info("Батч: прочитано %d, отправлено %d", len(messages), len(out))


if __name__ == "__main__":
    main()
