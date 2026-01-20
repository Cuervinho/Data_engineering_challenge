"""
Docstring for codes.silver

Script para la transformación de los datos orginales a una limpieza para la capa silver.

Este archivo se encarga de unificar los micro batchs de la capa bronze y aplicar reglas de
calidad y validación. Los registros que no cumplen las reglas se guardan en una carpeta
llamada cuarentena.
"""




import pandas as pd
import glob
import os
import logging
from datetime import datetime

# Logging básico
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger("silver-layer")

def run_silver():
    try:
        logger.info("Procesando capa Bronze para Silver")
        
        # Cargar todos los micro-batches
        files = glob.glob("data/bronze/**/*.parquet", recursive=True)
        if not files: 
            return logger.warning("No hay datos en Bronze.")
        
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

        # Filtro de registros válidos 
        mask_valid = (
                    df["loan_id"].notnull() & 
                    df["customer_id"].notnull() & 
                    (df["principal_amount"] > 0) 
                )
        
        df_silver = df[mask_valid].copy()
        df_cuarentena = df[~mask_valid].copy()

        # Eliminar duplicados
        df_silver = df_silver.drop_duplicates(subset=["event_id"], keep="last")

        # 4. Guardar resultados
        os.makedirs("data/silver", exist_ok=True)
        os.makedirs("data/cuarentena", exist_ok=True)
        
        df_silver.to_parquet("data/silver/ventas_limpias.parquet", index=False)
        if not df_cuarentena.empty:
            df_cuarentena.to_parquet("data/cuarentena/invalidos.parquet", index=False)

        logger.info("Validación finalizada. ")

    except Exception as e:
        logger.error("Error en Silver: %s", e)

if __name__ == "__main__":
    run_silver()