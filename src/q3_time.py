from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from collections import Counter

def extract_mentions(users):
    # Función que se ocupa de extraer los usuarios mencionados. 
    # En caso de que no existan, retorna vacío.
    if users is not None:
        return [user['username'] for user in users]
    return []

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    spark = SparkSession.builder.appName("LatamChallenge").getOrCreate()

    # Se lee el archivo JSON completo y se selecciona solo la columna de usuarios mencionados para optimizar el tiempo
    data = spark.read.json(file_path).select('mentionedUsers')
    # Se extraen los nombres de los usuarios
    mentions_rdd = data.rdd.flatMap(lambda row: extract_mentions(row['mentionedUsers']))
    # Se cuentan las veces que ha sido mencionado cada usuario
    mention_counts = mentions_rdd.map(lambda mention: (mention, 1)).reduceByKey(lambda a, b: a + b)

    # Se genera el top 10
    top_mentions = mention_counts.takeOrdered(10, key=lambda x: -x[1])

    return top_mentions
