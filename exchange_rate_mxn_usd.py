import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# === 1. Solicitud HTTP ===
token = "0e79c5be3ac868f81a70779c2a599540fbe4de9cec0b376eda3e109b6dc430ee"
url = "https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF43718/datos"  # Tipo de cambio FIX (MXN/USD)

headers = {
    "Bmx-Token": token
}

response = requests.get(url, headers=headers)
data = response.json()

# === 2. Extraer lista de datos (diccionarios) ===
datos = data['bmx']['series'][0]['datos']  # Cada elemento es {'fecha': ..., 'dato': ...}

# === 3. Iniciar SparkSession ===
spark = SparkSession.builder.master("local[*]").appName("ExchangeRateApp").getOrCreate()

# === 4. Definir esquema (opcional pero recomendado) ===
schema = StructType([
    StructField("fecha", StringType(), True),
    StructField("dato", StringType(), True)
])

# === 5. Crear DataFrame ===
df = spark.createDataFrame(datos, schema=schema)

# === 6. Mostrar resultados ===
df.show(truncate=False)