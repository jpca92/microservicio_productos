import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
from dotenv import load_dotenv
import pandas as pd

from models import PayloadProductos
from storage import get_dataframe, flush_if_needed

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="Microservicio Productos S3")


# --------------------------------------------------
# HEALTH
# --------------------------------------------------
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat()
    }


# --------------------------------------------------
# ENDPOINT PRINCIPAL
# --------------------------------------------------
@app.post("/productos")
async def recibir_productos(payload: PayloadProductos, request: Request):
    logger.info("--- POST /productos recibido ---")
    logger.info(f"--- Cliente: {request.client.host} ---")

    if not payload.productos:
        logger.warning("=== Payload vacío ===")
        raise HTTPException(status_code=400, detail="=== Payload vacío ===")

    try:
        # Información de entrada
        skus_recibidos = [p.sku for p in payload.productos]
        logger.info(f"--- Productos recibidos: {len(skus_recibidos)} ---")
        logger.info(f"--- SKUs: {skus_recibidos} ---")

        # Obtener DataFrame en memoria
        df = get_dataframe()
        size_before = len(df)
        logger.info(f"--- Tamaño DF antes del upsert: {size_before} ---")

        # Payload → DataFrame
        df_nuevo = pd.DataFrame([p.dict() for p in payload.productos]) \
            .astype(str).fillna("")

        # UPSERT
        if df.empty:
            df_final = df_nuevo
            inserts = len(df_nuevo)
            updates = 0
        else:
            df.set_index("sku", inplace=True)
            df_nuevo.set_index("sku", inplace=True)

            skus_existentes = set(df.index)
            skus_nuevos = set(df_nuevo.index)

            updates = len(skus_existentes & skus_nuevos)
            inserts = len(skus_nuevos - skus_existentes)

            logger.info(f"--- Updates: {updates} ---")
            logger.info(f"--- Inserts: {inserts} ---")

            df.update(df_nuevo)
            nuevos = df_nuevo.loc[~df_nuevo.index.isin(df.index)]

            df_final = pd.concat([df, nuevos]).reset_index()

        # Actualizar DF en memoria
        df.drop(df.index, inplace=True)
        for col in df_final.columns:
            df[col] = df_final[col]

        size_after = len(df)
        logger.info(f"---Tamaño DF después del upsert: {size_after} ---")

        # Flush si aplica
        flush_if_needed()

        return {
            "mensaje": "Productos procesados correctamente",
            "total_registros": size_after,
            "insertados": inserts,
            "actualizados": updates
        }

    except Exception as e:
        logger.exception("--- Error procesando productos ---")
        raise HTTPException(status_code=500, detail=str(e))
