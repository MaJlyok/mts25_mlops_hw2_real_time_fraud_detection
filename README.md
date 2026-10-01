# Real-Time Fraud Detection System
Система обнаружения мошеннических транзакций в реальном времени.
## Архитектура

```
interface (Streamlit) -> [transactions] -> preprocess.py -> [transactions_processed] -> scorer.py -> [scoring]

[scoring] -> score_writer -> Postgres  -> interface (вкладка «Результаты»)
```

Компоненты:
1. **`interface`** (Streamlit UI): имитирует поток транзакций. Читает CSV и отправляет каждую строку отдельным JSON-сообщением в топик `transactions`.
2. **`fraud_detector`**: один контейнер, в котором параллельно работают два отдельных скрипта:
   - `scripts/preprocess.py`: читает `transactions`, считает признаки, пишет в `transactions_processed`;
   - `scripts/scorer.py`: читает `transactions_processed`, скорит моделью, пишет в `scoring`.
   Сервис только делает inference, обучения в нём нет.
3. **Kafka**: Zookeeper, брокер, `kafka-setup` (создаёт топики), Kafka UI (порт 8080).
4. **`postgres`**: база данных в той же сети. Таблицу `scores` создаёт одноразовый сервис `postgres-setup` (команда `CREATE TABLE IF NOT EXISTS` прописана прямо в `docker-compose.yaml`), поэтому она появляется при каждом запуске, если её ещё нет.
5. **`score_writer`**: читает топик `scoring` и записывает `transaction_id`, `score`, `fraud_flag` в таблицу `scores`. Повторные сообщения не создают дублей (`ON CONFLICT DO NOTHING`).

Таблица `scores`:

| Колонка | Тип | Описание |
|---|---|---|
| `transaction_id` | TEXT, PRIMARY KEY | id транзакции |
| `score` | DOUBLE PRECISION | вероятность фрода |
| `fraud_flag` | SMALLINT | 1 = фрод |
| `created_at` | TIMESTAMPTZ | время записи в базу (по нему определяются «последние») |

## Признаки модели (9 штук)

`amount_log`, `distance_km`, `hour`, `day_of_week`, `population_city_log` (числовые) и `cat_id`, `merch`, `us_state`, `gender` (категориальные, CatBoost кодирует их сам).

Признаки считает функция `build_features` из `fraud_detector/src/features.py`. Порядок признаков, порог фрода и значения для заполнения пропусков хранятся в `fraud_detector/artifacts/meta.json`, модель лежит в `fraud_detector/artifacts/model.cbm`.

Битые сообщения (не JSON, нет нужных полей) пропускаются с предупреждением в логе.

## Настройки (переменные окружения `fraud_detector`)

| Переменная | По умолчанию |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` |
| `KAFKA_TRANSACTIONS_TOPIC` | `transactions` |
| `KAFKA_PROCESSED_TOPIC` | `transactions_processed` |
| `KAFKA_SCORING_TOPIC` | `scoring` |
| `LOG_LEVEL` | `INFO` |

Порог фрода хранится в `fraud_detector/artifacts/meta.json`.

## Запуск

**1. Скачайте проект и запустите**
```bash
git clone <URL_репозитория>
cd mts25_mlops_hw2_real_time_fraud_detection
docker compose up --build -d
```

- Streamlit UI: http://localhost:8501
- Kafka UI: http://localhost:8080

**2. Убедитесь, что сервисы запущены**
```bash
docker compose ps
docker compose logs fraud_detector
```
В логах должны быть две строки старта: `[preprocess] Старт: transactions -> transactions_processed` и `[scorer] Старт: transactions_processed -> scoring`.

**3. Отправьте транзакции**
- Откройте UI http://localhost:8501, загрузите `test.csv`  и нажмите «Отправить».

**4. Проверьте результат**
- Интерфейс: вкладка «Результаты», кнопка «Посмотреть результаты» показывает 10 последних транзакций с `fraud_flag = 1` и гистограмму скоров последних 100 транзакций.
- Kafka UI: топик `scoring`.
- Логи обработки батчей: `docker compose logs -f fraud_detector` (строки `Батч: прочитано N, отправлено N` и `Батч: прочитано N, оценено N`).
- База данных:
```bash
docker compose exec postgres psql -U fraud -d fraud -c "SELECT count(*), sum(fraud_flag) FROM scores;"
```

Если порты 8080, 8501 или 9095 заняты, освободите их или измените в `docker-compose.yaml`.
## Формат сообщений

Сообщение в `transactions`:
```json
{"transaction_id": "<uuid>", "data": {"transaction_time": "2019-09-14 02:46", "amount": 25.79, "...": "..."}}
```

Сообщение в `scoring`:
```json
{"transaction_id": "<uuid>", "score": 0.0003, "fraud_flag": 0}
```

## Структура проекта

```
.
├── fraud_detector/
│   ├── artifacts/          
│   ├── scripts/
│   │   ├── preprocess.py   
│   │   └── scorer.py       
│   ├── src/
│   │   ├── features.py     
│   │   └── config.py       
│   ├── Dockerfile
│   └── requirements.txt
├── score_writer/
│   ├── writer.py          
│   ├── Dockerfile
│   └── requirements.txt
├── interface/
│   ├── app.py             
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yaml
└── README.md
└── test.csv
```
