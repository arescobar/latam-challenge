from typing import List, Tuple

from pyspark.sql import SparkSession
from collections import Counter
import re

from memory_profiler import profile

def extract_mentions(users):
    # Función para extraer los usuarios necesarios
    if users is not None:
        return [user['username'] for user in users]
    return []

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    # Sesión de spark
    spark = SparkSession.builder.appName("LatamChallenge").getOrCreate()

    # Se lee el JSON completo y se selecciona solo la columna de usuarios mencionados para optimizar memoria
    data = spark.read.json(file_path).select('mentionedUsers')

    # Contador para contar cada mención
    mention_counter = Counter()

    # Se procesan los datos en diferentes particiones para optimizar la memoria
    for row in data.rdd.toLocalIterator():
        mentions = extract_mentions(row['mentionedUsers'])
        mention_counter.update(mentions)

    # Se genera el top 10 de los usuarios más mencionados
    top_mentions = mention_counter.most_common(10)

    return top_mentions

q3_memory("farmers-protest-tweets-2021-2-4.json")