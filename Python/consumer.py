from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, from_utc_timestamp, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Définir le schéma des données JSON
schema = StructType([
    StructField("id_transaction", StringType(), True),
    StructField("type_transaction", StringType(), True),
    StructField("montant", DoubleType(), True),
    StructField("devise", StringType(), True),
    StructField("date", StringType(), True),
    StructField("lieu", StringType(), True),
    StructField("moyen_paiement", StringType(), True),
    StructField("details", StructType([
        StructField("produit", StringType(), True),
        StructField("quantite", IntegerType(), True),
        StructField("prix_unitaire", DoubleType(), True)
    ])),
    StructField("utilisateur", StructType([
        StructField("id_utilisateur", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("adresse", StringType(), True),
        StructField("email", StringType(), True)
    ]))
])

# Initialisation de la session Spark
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExample") \
    .config("spark.hadoop.fs.native.lib", "false") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
spark.conf.set("spark.hadoop.io.native.lib", "false")

# Consommation des données de Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transaction") \
    .load()

# Désérialisation des données JSON
df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# Conversion du champ `date` au fuseau horaire local (Europe/Paris)
df = df.withColumn("date_local", from_utc_timestamp(to_timestamp(col("date")), "CET"))

# Transformation des colonnes
df = df.withColumn("devise", when(col("devise") == "USD", "EUR").otherwise(col("devise")))  # Conversion USD en EUR
df = df.withColumn("montant", when(col("devise") == "USD", col("montant") * lit(0.85)).otherwise(col("montant")))  # Ajuster le montant pour USD
df = df.withColumn("date", col("date").cast("date"))  # Convertir la date en type date

# Filtrage des transactions sans paiement en erreur
df = df.filter(col("moyen_paiement") != "erreur")

# Filtrage spécifique pour utilisateur.adresse
df = df.filter((col("utilisateur.adresse").isNotNull()) & (col("utilisateur.adresse") != "None"))

# Suppression des lignes contenant des valeurs None
df = df.dropna(how='any')

# Afficher les données
df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "metadata") \
    .option("path", "s3a://warehouse/elem")  \
    .start() \
    .awaitTermination()

spark.stop()






