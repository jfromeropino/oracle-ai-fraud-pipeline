import oracledb
import pandas as pd
import time
import sys
import polars as pl
import requests
import json
import os
from dotenv import load_dotenv    

db_config = {
    "user": "DATAE",
    "password": "datae", 
    "dsn": "localhost:1521/XEPDB1" 
}

def conectar_y_extraer():
    conn = None 
    try:
        conn = oracledb.connect(
            user=db_config["user"],
            password=db_config["password"],
            dsn=db_config["dsn"]
        )
        print("Conexión establecida con BD Oracle 21c XE.")

        sql = "SELECT * FROM DATAE.TRANSACCIONES"
        tamanio_lote = 100000
        lista_dataframes = []
        
        print(f"Extrayendo datos en lotes de {tamanio_lote}...")

        for i, chunk in enumerate(pd.read_sql(sql, conn, chunksize=tamanio_lote)):
            lista_dataframes.append(chunk)
            print(f" Lote {i+1} procesado ({len(chunk)} filas)...")

        df_final = pd.concat(lista_dataframes, ignore_index=True)
        return df_final

    except Exception as e:
        print(f"Error durante la extracción: {str(e)}")
        return None
    finally:
        if conn:
            conn.close()
            print("Conexión a BD cerrada.")

def ejecutar_transformacion(df_pandas):
    if df_pandas is None or df_pandas.empty:
        return None, None      
    
    print("\n--- Iniciando Transformación con Polars ---")
    # Conversión de Pandas a Polars
    df = pl.from_pandas(df_pandas)

    # Pipeline de Transformación
    df_resultado = df.with_columns([
        pl.col("COMENTARIO").str.to_lowercase().str.strip_chars().alias("COMENTARIO_LIMPIO"),
        (pl.col("MONTO") * 0.19).alias("IMPUESTO_IVA"),
        pl.when((pl.col("MONTO") > 4000) & (pl.col("ESTADO") == "F"))
        .then(pl.lit("ALTO RIESGO"))
        .otherwise(pl.lit("NORMAL"))
        .alias("CATEGORIA_RIESGO")
    ])

    # Filtrar alertas
    alertas = df_resultado.filter(pl.col("CATEGORIA_RIESGO") == "ALTO RIESGO")
    
    return df_resultado, alertas

def analizar_riesgos_con_ia(df_alertas):
    load_dotenv() 
    api_key = os.getenv("DEEPSEEK_API_KEY")
    
    print("\n Enviando muestras de riesgo (10) al Agente de IA...")
    
    # Convertir las primeras 10 alertas a una lista de diccionarios
    muestras = df_alertas.head(10).select(["ID_TRANSACCION", "MONTO", "COMENTARIO_LIMPIO"]).to_dicts()
    
    # Configuración IA DeepSeek (OpenRouter)
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}", 
        "Content-Type": "application/json"
    }
    
    prompt = f"""
    Como experto en seguridad bancaria, analiza estas transacciones marcadas como FRAUDE con montos altos:
    {json.dumps(muestras, indent=2)}
    
    Dime:
    1. ¿Qué patrones de riesgo detectas en los comentarios?
    2. ¿Cuál es el monto promedio de estas alertas?
    3. Dame una recomendación breve para el equipo de auditoría.
    """
    
    payload = {
        "model": "deepseek/deepseek-chat",
        "messages": [{"role": "user", "content": prompt}]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        resultado = response.json()
        # Si hay un error en la respuesta, lo imprimimos para saber qué es
        if 'error' in resultado:
            return f"⚠️ Error de la API: {resultado['error']['message']}"
    
        return resultado['choices'][0]['message']['content']
    except Exception as e:
        return f"❌ Error de conexión o formato: {str(e)}"


if __name__ == "__main__":
    inicio_total = time.time()
    
    # 1. EXTRACCIÓN
    df = conectar_y_extraer()
    
    # 2. VALIDACIÓN Y TRANSFORMACIÓN
    if df is not None:
        df_final, alertas = ejecutar_transformacion(df)
        
        if df_final is not None:            
            if len(alertas) > 0:
                print("\n Alertas de Alto Riesgo:")
                print(alertas.select(["ID_TRANSACCION", "MONTO", "COMENTARIO_LIMPIO"]).head(10))

                respia = analizar_riesgos_con_ia(alertas)
                
            
            # Almacenamiento de alertas en parquet
            alertas.write_parquet("alertas_fraude_criticas.parquet")
            print("Alertas guardadas exitosamente en 'alertas_fraude_criticas.parquet'")
            
            print(f" Total procesado: {len(df_final)} filas.")
            print(f" Alertas críticas: {len(alertas)}")
            print("\n Análisis de IA (DeepSeek):")
            print(respia)
            fin_total = time.time()
            print(f" Tiempo total del pipeline: {round(fin_total - inicio_total, 2)} segundos.")
    else:
        print("El pipeline se detuvo porque no se pudieron obtener datos de la BD.")