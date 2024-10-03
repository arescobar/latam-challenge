from typing import List, Tuple
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format, row_number
from pyspark.sql.window import Window
from typing import List, Tuple
import datetime
from memory_profiler import profile

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    
    # Se inicia la sesión en pyspark 
    spark = SparkSession.builder.appName("LatamChallenge").getOrCreate()

        # Se leen solo las columnas necesarias para reducir el uso de memoria
    try:
        data = spark.read.json(file_path).select('date', 'user.username')
    except (FileNotFoundError, IOError) as e:
        print(f'Error while handling file: {e}')
    # Se convierte la columna 'date' a solo la fecha
    data = data.withColumn('created_at_date', date_format(col('date'), 'yyyy-MM-dd'))
    # Se agrupa por fecha y usuario para contar los tweets de cada usuario por día
    tweet_counts = data.groupBy('created_at_date', 'username').agg(count('*').alias('tweet_count'))

    # Se define una ventana para obtener el usuario con más tweets por fecha
    window = Window.partitionBy('created_at_date').orderBy(col('tweet_count').desc())

    # Se añade una columna de ranking para identificar el usuario con más tweets por cada fecha
    ranked_data = tweet_counts.withColumn('rank', row_number().over(window))

    # Se filtran para quedarnos solo con el usuario que tiene más tweets por cada fecha
    top_users_per_date = ranked_data.filter(col('rank') == 1).select('created_at_date', 'username')

    # Se cuenta el total de tweets por fecha y se ordenan para obtener las 10 fechas con más tweets
    top_dates = data.groupBy('created_at_date').agg(count('*').alias('total_tweets')).orderBy(col('total_tweets').desc()).limit(10)

    # Se unen las tablas para obtener los usuarios correspondientes a esas fechas
    top_result = top_dates.join(top_users_per_date, on='created_at_date', how='inner') \
                          .select('created_at_date', 'username')

    # Se retorna una tupla con el resultado
    result_list = [(row['created_at_date'], row['username']) for row in top_result.collect()]
    return result_list


#q1_memory("farmers-protest-tweets-2021-2-4.json")