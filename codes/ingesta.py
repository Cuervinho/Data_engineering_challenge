import pandas as pd
import os
import time
from datetime import datetime

# Configuración de rutas
SOURCE_PATH = "source_data/credit_events.csv"
TARGET_BASE_PATH = "data/bronze"

def streaming(batch_size=1000, sleep_time=15):
    """
    Obtiene el precio y la información bursátil actual de una acción
    usando el servicio Finnhub.

    Argumentos:
        symbol (str): simbolo en los mercados de la acción.
        api_key (str): clave de acceso personal para la API de Finnhub.
        max_retries (int, optional): El número máximo de veces que intentará
                                     llamar al servicio antes de rendirse.
                                     Por defecto son 5 intentos.

    Retorna:
        Un diccionario con la información del precio del activo:
        'c' (precio actual), 'o' (precio de apertura), 'h' (precio máximo),
        'l' (precio mínimo), 'pc' (precio de cierre anterior) y 't' (timestamp).
        Devuelve 'None' si se agotan todos los reintentos
        o si ocurre un error grave e inesperado.
    """
    if not os.path.exists(SOURCE_PATH):
        print(f"No se encontró {SOURCE_PATH}")
        return
    
    print(f"Iniciando ingesta con Pandas...")
    
    # 1. Leer el archivo fuente
    df_source = pd.read_csv(SOURCE_PATH)
    total_rows = len(df_source)
    
    start = 0
    batch_num = 1

    while start < total_rows:
        # Extraer el micro batch
        end = start + batch_size
        batch = df_source.iloc[start:end].copy()

        # Añadir Metadatos para la capa bronze
        now = datetime.now()
        batch['ingestion_timestamp'] = now
        batch['source_file'] = SOURCE_PATH
        
        # Extraer año, mes, día para el particionado manual
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")

        # Particionado razonable
        partition_path = os.path.join(TARGET_BASE_PATH, f"year={year}", f"month={month}", f"day={day}")
        os.makedirs(partition_path, exist_ok=True)

        # Guardar en Parquet 
        filename = f"batch_{now.strftime('%H%M%S_%f')}.parquet"
        file_path = os.path.join(partition_path, filename)
        
        batch.to_parquet(file_path, engine='pyarrow', index=False)

        print(f"[{now.strftime('%H:%M:%S')}] Batch {batch_num} guardado. ({end}/{total_rows} filas)")

        start = end
        batch_num += 1
        
        if start < total_rows:
            time.sleep(sleep_time)

    print("Simulación finalizada. Datos guardados en Capa Bronze.")

if __name__ == "__main__":
    streaming()