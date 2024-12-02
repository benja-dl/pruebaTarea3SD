from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Leer datos desde Kafka
kafka_streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "waze_traffic") \
    .option("startingOffsets", "earliest") \
    .load()

# Procesar los datos: Convertir de binario a string
processed_df = kafka_streaming_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Escribir los datos procesados en la consola para verificar
query = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
