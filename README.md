# 🚀 Oracle-AI Fraud Detection Pipeline

Este proyecto es un pipeline de datos de alto rendimiento que conecta sistemas financieros tradicionales (Oracle) con Inteligencia Artificial moderna.

## 🛠️ Tecnologías Utilizadas
- **Base de Datos:** Oracle 21c XE (Extracción masiva con PL/SQL).
- **Procesamiento de Datos:** **Polars** (Motor de alto rendimiento en Rust) y Pandas.
- **Interoperabilidad:** Apache Arrow (PyArrow) para transferencia de datos "Zero-Copy".
- **IA Generativa:** DeepSeek-V3 (vía OpenRouter API) para el análisis semántico de fraude.
- **Almacenamiento:** Apache Parquet (Formato columnar de Big Data).

## 📊 Capacidades del Proyecto
1. **Extracción Eficiente:** Manejo de 500,000 registros mediante *chunking* para optimizar el uso de RAM.
2. **Transformación Veloz:** Procesamiento de lógica de negocio y cálculo de impuestos en milisegundos usando Polars.
3. **Análisis de IA:** Un agente de IA analiza los comentarios de las transacciones marcadas como riesgo para identificar patrones de ataques automatizados o suplantación.

## 🚀 Cómo ejecutarlo
1. Configurar un entorno virtual: `python -m venv venv`.
2. Instalar dependencias: `pip install oracledb pandas polars pyarrow requests`.
3. Configurar las credenciales de Oracle en `db_config`.
4. Ejecutar: `python analisis.py`.