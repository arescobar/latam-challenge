from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split
from collections import Counter
import re

import cProfile 

def extract_emojis(text):
    # Se utiliza la misma función para identificar los emojis
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticonos
                           u"\U0001F300-\U0001F5FF"  # símbolos y pictogramas
                           u"\U0001F680-\U0001F6FF"  # transportes y símbolos
                           u"\U0001F1E0-\U0001F1FF"  # banderas
                           "]+", flags=re.UNICODE)
    return emoji_pattern.findall(text)

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    spark = SparkSession.builder.appName("LatamChallenge").getOrCreate()

    # Se lee el archivo JSON completo
    data = spark.read.json(file_path).select('content')
    # Se usa un RDD para extraer emojis rápidamente
    emojis_rdd = data.rdd.flatMap(lambda row: extract_emojis(row['content']))
    # Se cuenta la cantidad de veces que aparece cada emoji
    emoji_counts = emojis_rdd.map(lambda emoji: (emoji, 1)).reduceByKey(lambda a, b: a + b)
    # Top emojis
    top_emojis = emoji_counts.takeOrdered(10, key=lambda x: -x[1])

    return top_emojis

if __name__ == "__main__":
    cProfile.run('q2_time("farmers-protest-tweets-2021-2-4.json")')