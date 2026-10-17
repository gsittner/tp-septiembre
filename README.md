🏆 GDELT Sports Data Pipeline & Sentiment Analysis

Análisis de la agenda deportiva en el ecosistema de medios hispanohablantes a gran escala.
Procesamiento distribuido, extracción de entidades y análisis de sentimiento léxico utilizando datos globales de GDELT GKG.

📑 Tabla de Contenidos

Contexto y Objetivo

Arquitectura del Pipeline

Stack Tecnológico

Estructura del Repositorio

Instrucciones de Ejecución

Autores

🎯 Contexto y Objetivo

Este proyecto implementa un flujo de trabajo End-to-End de ingeniería y ciencia de datos. Su propósito es monitorear el pulso de las noticias deportivas en países de habla hispana, ingiriendo grandes volúmenes de datos desde la base global GDELT Project (Global Database of Events, Language, and Tone).

El sistema clasifica artículos por región y disciplina, evalúa la polaridad léxica (tono emocional de la cobertura), enriquece los datos mediante web scraping directo y expone los resultados en dashboards interactivos.

⚙️ Arquitectura del Pipeline

El flujo de los datos sigue un diseño robusto orientado a Big Data:

Ingesta y Extracción (ETL): Descarga multihilo de 672 lotes en formato ZIP correspondientes a los últimos 7 días de eventos globales en GDELT.

Filtrado Taxonómico: Limpieza y retención de URLs usando diccionarios de dominios deportivos, entidades (ej. FIFA, NBA) y palabras clave en español.

Deep Scraping (Módulo Extra): Web scraping focalizado a los portales de ESPN extrayendo el contenido textual completo (DOM HTML) para NLP profundo.

Procesamiento Distribuido: Inicialización de clúster local con Apache Spark para agregaciones complejas, Window Functions y cálculo de variaciones temporales.

Almacenamiento Columnar: Escritura de los DataFrames procesados en formato Apache Parquet, aplicando particionado físico por país (partitionBy("pais")) para optimizar consultas.

Data Visualization: Consolidación de métricas y renderizado de interfaces gráficas interactivas incrustadas.

🛠️ Stack Tecnológico

Extracción y Web Scraping:

requests / concurrent.futures (Conexiones HTTP y Multithreading)

BeautifulSoup / bs4 (Parsing HTML)

Data Engineering & Big Data:

PySpark (Apache Spark API para procesamiento distribuido)

Spark SQL & Window Functions

Apache Parquet (Compresión Snappy y almacenamiento columnar)

Data Analysis & Visualización:

Pandas (Manipulación de métricas agregadas)

Plotly (Express & Graph Objects para dashboards y mapas coropléticos)

HTML/CSS (Maquetación de la interfaz interactiva)

📁 Estructura del Repositorio

📦 gdelt-sports-pipeline
 ┣ 📜 sittner-salvatore-campos-mareco.ipynb       # 📊 Presentación interactiva y dashboards
 ┣ 📜 sittner_salvatore_campos_mareco_crudo_2.py  # ⚙️ Motor ETL, Scraping y PySpark (Backend)
 ┣ 📜 README.md                                   # 📖 Documentación del proyecto
 ┗ 📂 data/                                       # (Generado localmente)
   ┣ 📜 gdelt_deportes_semana_completa.csv        # Extracción cruda deduplicada
   ┗ 📂 gdelt_deportes_semana_completa.parquet/   # Data Lake columnar particionado


Nota: Para auditorías de código y evaluación de la arquitectura de ingeniería (PySpark, Parquet, Web Scraping), referirse al script .py. Para la evaluación de negocio y visualización, abrir el notebook .ipynb.

🚀 Instrucciones de Ejecución

Opción A: Google Colab (Recomendada)

El entorno ideal para probar la interfaz visual interactiva sin configuraciones locales complejas.

Subir el archivo sittner-salvatore-campos-mareco.ipynb a Google Colab.

Ejecutar las celdas en orden. El script detectará el entorno, instalará dependencias, descargará los lotes en vivo e inicializará la sesión de Spark de forma transparente.

Opción B: Entorno Local (Para Ingenieros de Datos)

Ideal para testear la generación del particionado Parquet y el scraping.

Requisitos previos: Java 8+ y Apache Spark instalados y en el PATH.

Crear un entorno virtual e instalar dependencias:

pip install pyspark pandas plotly seaborn matplotlib beautifulsoup4 requests


Ejecutar el motor de datos:

python sittner_salvatore_campos_mareco_crudo_2.py


👥 Autores

Sittner

Salvatore

Campos

Mareco

Proyecto desarrollado para demostrar competencias integrales en la ingeniería y visualización de grandes volúmenes de datos.
