import oracledb
import pandas as pd
import time
import sys
import polars as pl
import requests
import json

db_config = {
    "user": "DATAE",
    "password": "datae", 
    "dsn": "localhost:1521/XEPDB1" # Asegúrate que sea XEPDB1 o xe según tu config
}

def conectar_y_extraer():
    conn = None # Inicializamos para asegurar el cierre
    try:
        conn = oracledb.connect(
            user=db_config["user"],
            password=db_config["password"],
            dsn=db_config["dsn"]
        )
        print("✅ Conexión establecida con Oracle 21c XE.")

        sql = "SELECT * FROM DATAE.TRANSACCIONES"
        tamanio_lote = 100000
        lista_dataframes = []
        
        print(f"🚀 Extrayendo datos en lotes de {tamanio_lote}...")

        for i, chunk in enumerate(pd.read_sql(sql, conn, chunksize=tamanio_lote)):
            lista_dataframes.append(chunk)
            print(f"📦 Lote {i+1} procesado ({len(chunk)} filas)...")

        df_final = pd.concat(lista_dataframes, ignore_index=True)
        return df_final

    except Exception as e:
        print(f"❌ Error durante la extracción: {str(e)}")
        return None
    finally:
        if conn:
            conn.close()
            print("🔒 Conexión a Oracle cerrada.")

def ejecutar_transformacion(df_pandas):
    if df_pandas is None or df_pandas.empty:
        return None, None      
    
    print("\n--- ⚡ Iniciando Transformación con Polars ---")
    # 1. Convertir de Pandas a Polars (Cero copia)
    df = pl.from_pandas(df_pandas)

    # 2. Pipeline de Transformación
    df_resultado = df.with_columns([
        pl.col("COMENTARIO").str.to_lowercase().str.strip_chars().alias("COMENTARIO_LIMPIO"),
        (pl.col("MONTO") * 0.19).alias("IMPUESTO_IVA"),
        pl.when((pl.col("MONTO") > 4000) & (pl.col("ESTADO") == "F"))
        .then(pl.lit("ALTO RIESGO"))
        .otherwise(pl.lit("NORMAL"))
        .alias("CATEGORIA_RIESGO")
    ])

    # 3. Filtrar alertas
    alertas = df_resultado.filter(pl.col("CATEGORIA_RIESGO") == "ALTO RIESGO")
    
    return df_resultado, alertas

def analizar_riesgos_con_ia(df_alertas):
    print("\n🤖 Enviando muestras de riesgo al Agente de IA...")
    
    # Tomamos una muestra representativa de las alertas para no saturar el prompt
    # Convertimos las primeras 10 alertas a una lista de diccionarios
    muestras = df_alertas.head(10).select(["ID_TRANSACCION", "MONTO", "COMENTARIO_LIMPIO"]).to_dicts()
    
    # Preparamos el contexto para DeepSeek (OpenRouter)
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": "Bearer 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", # Tu clave
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
        return resultado['choices'][0]['message']['content']
    except Exception as e:
        return f"Error al conectar con la IA: {e}"


if __name__ == "__main__":
    inicio_total = time.time()
    
    # 1. EXTRACCIÓN
    df_crudo = conectar_y_extraer()
    
    # 2. VALIDACIÓN Y TRANSFORMACIÓN
    if df_crudo is not None:
        df_final, alertas = ejecutar_transformacion(df_crudo)
        
        if df_final is not None:
            fin_total = time.time()
            print(f"\n--- 📊 RESULTADOS FINALES ---")
            print(f"✅ Total procesado: {len(df_final)} filas.")
            print(f"🚨 Alertas críticas: {len(alertas)}")
            print(f"⏱️ Tiempo total del pipeline: {round(fin_total - inicio_total, 2)} segundos.")
            
            # Mostrar las primeras 5 alertas para verificar
            if len(alertas) > 0:
                print("\n🔥 Muestra de Alertas de Alto Riesgo:")
                print(alertas.select(["ID_TRANSACCION", "MONTO", "COMENTARIO_LIMPIO"]).head(10))

                respia = analizar_riesgos_con_ia(alertas)
                print("\n🤖 Análisis de IA:")
                print(respia)
            
            # Guardar las alertas en formato profesional de Big Data
            alertas.write_parquet("alertas_fraude_criticas.parquet")
            print("💾 Alertas guardadas exitosamente en 'alertas_fraude_criticas.parquet'")
    else:
        print("🛑 El pipeline se detuvo porque no se pudieron obtener datos de Oracle.")