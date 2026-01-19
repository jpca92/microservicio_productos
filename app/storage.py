import io
import os
import time
import threading
import boto3
import pandas as pd
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOCAL_PARQUET = os.path.join(DATA_DIR, "datos_endpoint.parquet")

BUCKET = os.getenv("S3_BUCKET")
S3_KEY = "DianaAC/ChatBotRAG/data_endpoint/datos_endpoint.parquet"

FLUSH_INTERVAL = 15  # segundos

os.makedirs(DATA_DIR, exist_ok=True)

s3_client = boto3.client(
    "s3",
    region_name=os.getenv("S3_REGION")
)

# --------------------------------------------------
# CACHE EN MEMORIA
# --------------------------------------------------
_df_cache: pd.DataFrame | None = None
_last_flush = 0
_lock = threading.Lock()


# --------------------------------------------------
# LOAD
# --------------------------------------------------
def _load_dataframe_from_disk_or_s3() -> pd.DataFrame:
    if os.path.exists(LOCAL_PARQUET):
        logger.info("--- Cargando parquet desde disco local ---")
        df = pd.read_parquet(LOCAL_PARQUET)
        logger.info(f"--- Registros cargados desde disco: {len(df)} ---")
        return df.astype(str).fillna("")

    try:
        logger.info("--- Parquet local no existe, descargando desde S3 ---")
        obj = s3_client.get_object(Bucket=BUCKET, Key=S3_KEY)
        buffer = io.BytesIO(obj["Body"].read())

        df = pd.read_parquet(buffer).astype(str).fillna("")
        logger.info(f"--- Registros descargados desde S3: {len(df)} ---")

        df.to_parquet(LOCAL_PARQUET, index=False)
        logger.info("--- Parquet guardado en disco local ---")

        return df

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.warning("=== No existe parquet ni en local ni en S3 ===")
            return pd.DataFrame()
        raise


# --------------------------------------------------
# PUBLIC API
# --------------------------------------------------
def get_dataframe() -> pd.DataFrame:
    global _df_cache
    with _lock:
        if _df_cache is None:
            logger.info("=== Inicializando DataFrame en memoria ===")
            _df_cache = _load_dataframe_from_disk_or_s3()
        return _df_cache


def flush_if_needed():
    global _last_flush, _df_cache

    if _df_cache is None:
        return

    now = time.time()
    if now - _last_flush < FLUSH_INTERVAL:
        return

    with _lock:
        logger.info("--- Ejecutando flush del DataFrame ---")

        _df_cache.to_parquet(LOCAL_PARQUET, index=False)
        logger.info(f"--- Guardado local ({len(_df_cache)} registros) ---")

        buffer = io.BytesIO()
        _df_cache.to_parquet(buffer, index=False)

        s3_client.put_object(
            Bucket=BUCKET,
            Key=S3_KEY,
            Body=buffer.getvalue(),
            ContentType="application/parquet"
        )

        _last_flush = now
        logger.info("--- Parquet actualizado en S3 ---")
