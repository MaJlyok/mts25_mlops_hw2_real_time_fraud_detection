
import numpy as np
import pandas as pd


FEATURE_COLUMNS = [
    "amount_log",           # log(1 + сумма транзакции)
    "distance_km",          # расстояние клиент - магазин, км
    "hour",                 # час суток (0-23)
    "day_of_week",          # день недели (0=пн ... 6=вс)
    "cat_id",               # категория магазина (категориальный)
    "merch",                # название магазина (категориальный)
    "us_state",             # штат (категориальный)
    "gender",               # пол (категориальный)
    "population_city_log",  # log(1 + население города)
]

CATEGORICAL_COLUMNS = ["cat_id", "merch", "us_state", "gender"]

RAW_REQUIRED_COLUMNS = [
    "transaction_time", "amount", "lat", "lon", "merchant_lat", "merchant_lon",
    "population_city", "cat_id", "merch", "us_state", "gender",
]

MISSING_CATEGORY = "NA" # Значение-заглушка для пропусков в категориях для CatBoost 

EARTH_RADIUS_KM = 6371.0088 # Радиус Земли в км - нужен для формулы haversine.


def haversine_km(lat1, lon1, lat2, lon2):
    """
    Расстояние по дуге большого круга между двумя точками (в км).
    """
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2)) 
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def build_features(df, impute_values):
    """
    Преобразует сырые транзакции в матрицу признаков для модели.

    Returns
    -------
    pd.DataFrame c колонками ровно в порядке FEATURE_COLUMNS.
    """
    missing = [c for c in RAW_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"В данных не хватает колонок: {missing}")

    out = pd.DataFrame(index=df.index)

    ts = pd.to_datetime(df["transaction_time"], errors="coerce")
    out["hour"] = ts.dt.hour
    out["day_of_week"] = ts.dt.dayofweek
    amount = pd.to_numeric(df["amount"], errors="coerce")
    population = pd.to_numeric(df["population_city"], errors="coerce")

    out["amount_log"] = np.log1p(amount.clip(lower=0))
    out["population_city_log"] = np.log1p(population.clip(lower=0))

    out["distance_km"] = haversine_km(
        pd.to_numeric(df["lat"], errors="coerce"),
        pd.to_numeric(df["lon"], errors="coerce"),
        pd.to_numeric(df["merchant_lat"], errors="coerce"),
        pd.to_numeric(df["merchant_lon"], errors="coerce"),
    )

    for col in CATEGORICAL_COLUMNS:
        out[col] = df[col].astype("object").where(df[col].notna(), MISSING_CATEGORY).astype(str) # заменяем NaN на MISSING_CATEGORY

    # --- Заполнение пропусков в числовых признаках 
    for col, value in impute_values.items():
        out[col] = out[col].fillna(value)

    out[["hour", "day_of_week"]] = out[["hour", "day_of_week"]].fillna(0)

    return out[FEATURE_COLUMNS]
