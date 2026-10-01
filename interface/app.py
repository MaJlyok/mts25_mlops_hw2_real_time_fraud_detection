import streamlit as st
import pandas as pd
import altair as alt
import psycopg2
from kafka import KafkaProducer
import json
import os
import uuid

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT",
            linger_ms=50,
            batch_size=262144,
            compression_type="gzip",
        )
        
        df = df.astype(object).where(df.notna(), None)

        records = df.to_dict(orient="records")
        total_rows = len(records)
        progress_bar = st.progress(0)

        for i, record in enumerate(records, start=1):
            # Уникальный ID для каждой транзакции
            producer.send(
                topic,
                value={"transaction_id": str(uuid.uuid4()), "data": record}
            )
            if i % 2000 == 0 or i == total_rows:
                progress_bar.progress(i / total_rows)

        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

def load_results():
    """Читает из Postgres: 10 последних фродов и скоры 100 последних транзакций"""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "fraud"),
        user=os.getenv("POSTGRES_USER", "fraud"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT transaction_id, score, fraud_flag, created_at FROM scores "
                "WHERE fraud_flag = 1 ORDER BY created_at DESC LIMIT 10"
            )
            frauds = pd.DataFrame(
                cur.fetchall(), columns=["transaction_id", "score", "fraud_flag", "created_at"]
            )
            cur.execute("SELECT score FROM scores ORDER BY created_at DESC LIMIT 100")
            last = pd.DataFrame(cur.fetchall(), columns=["score"])
    finally:
        conn.close()
    return frauds, last

# Интерфейс
st.title("Детектор фрода")
tab_send, tab_results = st.tabs(["Отправка данных", "Результаты"])

with tab_send:
    st.header("📤 Отправка данных в Kafka")

    if "uploaded_files" not in st.session_state:
        st.session_state.uploaded_files = {}

    # Блок загрузки файлов
    uploaded_file = st.file_uploader(
        "Загрузите CSV файл с транзакциями",
        type=["csv"]
    )

    if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
        # Добавляем файл в состояние
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": load_file(uploaded_file)
        }
        st.success(f"Файл {uploaded_file.name} успешно загружен!")

    # Список загруженных файлов
    if st.session_state.uploaded_files:
        st.subheader("🗂 Список загруженных файлов")

        for file_name, file_data in st.session_state.uploaded_files.items():
            cols = st.columns([4, 2, 2])

            with cols[0]:
                st.markdown(f"**Файл:** `{file_name}`")
                st.markdown(f"**Статус:** `{file_data['status']}`")

            with cols[2]:
                if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                    if file_data["df"] is not None:
                        with st.spinner("Отправка..."):
                            success = send_to_kafka(
                                file_data["df"],
                                KAFKA_CONFIG["topic"],
                                KAFKA_CONFIG["bootstrap_servers"]
                            )
                            if success:
                                st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                                st.rerun()
                    else:
                        st.error("Файл не содержит данных")

with tab_results:
    st.header("📊 Результаты скоринга")

    if st.button("Посмотреть результаты"):
        try:
            frauds, last = load_results()
        except Exception as e:
            st.error(f"Ошибка чтения из базы: {str(e)}")
        else:
            st.subheader("10 последних транзакций с fraud_flag = 1")
            if frauds.empty:
                st.info("Фродовых транзакций пока нет")
            else:
                st.dataframe(frauds, use_container_width=True)

            st.subheader(f"Распределение скоров последних {len(last)} транзакций")
            if last.empty:
                st.info("В базе пока нет транзакций")
            else:
                chart = alt.Chart(last).mark_bar().encode(
                    x=alt.X("score:Q", bin=alt.Bin(maxbins=20, extent=[0, 1]), title="Score"),
                    y=alt.Y("count()", title="Количество транзакций"),
                )
                st.altair_chart(chart, use_container_width=True)