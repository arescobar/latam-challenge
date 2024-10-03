from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split, count
from collections import Counter
import re

from memory_profiler import profile

def extract_emojis(text):
    # Función para extraer los emojis de los tweets
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticonos
                           u"\U0001F300-\U0001F5FF"  # símbolos y pictogramas
                           u"\U0001F680-\U0001F6FF"  # transportes y símbolos
                           u"\U0001F1E0-\U0001F1FF"  # banderas
                           "]+", flags=re.UNICODE)
    return emoji_pattern.findall(text)

@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    spark = SparkSession.builder.appName("LatamChallenge").getOrCreate()

    # Nuevamente se lee solo la columna de contenido para reducir el uso de memoria
    data = spark.read.json(file_path).select('content')
    # Se filtran los tweets con emojis
    tweets_with_emojis = data.rdd.flatMap(lambda row: extract_emojis(row['content']))
    # Se cuenta la cantidad de veces que aparece cada emoji
    emoji_counts = tweets_with_emojis.map(lambda emoji: (emoji, 1)).reduceByKey(lambda a, b: a + b)
    # Top 10 emojis
    top_emojis = emoji_counts.takeOrdered(10, key=lambda x: -x[1])

    return top_emojis

q2_memory("farmers-protest-tweets-2021-2-4.json")