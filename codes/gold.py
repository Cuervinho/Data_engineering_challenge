"""
Docstring for codes.gold

Script para la generación de datos analíticos

Este archivo realiza la unión de los datos de créditos con información de 
regiones y riesgo. Construye métricas agregadas por cohorte y una vista 
del estado de la cartera.
"""


import pandas as pd
import os
import logging

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger("gold-layer")

def run_gold():
    try:
        logger.info("Generando Capa Gold")

        # Cargan datasets
        df_silver = pd.read_parquet("data/silver/ventas_limpias.parquet")
        df_regiones = pd.read_csv("source_data/region_reference.csv") 

        # Unimos con el dataset de regiones
        df_gold = pd.merge(df_silver, df_regiones, on="region", how="left")
        
        # Derivamos la cohorte (Mes/Año)
        df_gold['cohorte'] = pd.to_datetime(df_gold['event_time']).dt.to_period('M').astype(str)

        # Metricas por Cohorte
        reporte_cohorte = df_gold.groupby('cohorte').agg(
            total_prestado=('principal_amount', 'sum'),
            cantidad_creditos=('loan_id', 'count')
        ).reset_index()

        # Estado de los creditos
        vista_consolidada = df_gold.groupby(['macro_region', 'loan_status']).agg(
            saldo_pendiente=('outstanding_balance', 'sum'),
            mora_promedio=('days_past_due', 'mean')
        ).reset_index()

        # Guardar data gold
        os.makedirs("data/gold", exist_ok=True)
        reporte_cohorte.to_parquet("data/gold/reporte_cohorte.parquet", index=False)
        vista_consolidada.to_parquet("data/gold/vista_consolidada.parquet", index=False)

        logger.info("Capa Gold finalizada.")
        print(vista_consolidada)

    except Exception as e:
        logger.error("Error en Gold: %s", e)

if __name__ == "__main__":
    run_gold()