import json
import sys
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier
from confluent_kafka import Consumer, Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import config 

logger = config.setup_logging("scorer")


def score_messages(items, model, meta):
    """
    items: список словарей {"transaction_id", "features"}.
    Возвращает список результатов {"transaction_id", "score", "fraud_flag"}.
    """
    if not items:
        return []
    # Колонки берём в порядке из meta.json - в том же, в каком учили модель.
    features = pd.DataFrame([i["features"] for i in items])[meta["feature_columns"]]
    scores = model.predict_proba(features)[:, 1]
    return [
        {
            "transaction_id": item["transaction_id"],
            "score": float(score),
            "fraud_flag": int(score >= meta["threshold"]),
        }
        for item, score in zip(items, scores)
    ]


def parse_messages(raw_values):
    """Разбирает JSON; битые сообщения логирует и пропускает."""
    items = []
    for raw in raw_values:
        try:
            item = json.loads(raw.decode("utf-8"))
            items.append(item)
        except Exception as e: 
            logger.warning("Пропускаем битое сообщение: %s", e)
    return items


def main():
    meta = config.load_meta()
    model = CatBoostClassifier()
    model.load_model(str(config.MODEL_PATH))

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "scorer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  
    })
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([config.PROCESSED_TOPIC])

    logger.info(
        "Старт: %s -> %s, порог=%.3f",
        config.PROCESSED_TOPIC, config.SCORING_TOPIC, meta["threshold"],
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

        results = score_messages(parse_messages(valid), model, meta)

        for r in results:
            producer.produce(
                config.SCORING_TOPIC,
                key=str(r["transaction_id"]),
                value=json.dumps(r).encode("utf-8"),
            )
        producer.flush()
        consumer.commit(asynchronous=False)
        logger.info("Батч: прочитано %d, оценено %d", len(messages), len(results))


if __name__ == "__main__":
    main()
