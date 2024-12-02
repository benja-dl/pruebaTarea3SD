from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("SparkTest") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Crear un DataFrame de prueba
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
df = spark.createDataFrame(data, ["Name", "Value"])

# Mostrar el contenido del DataFrame
df.show()
