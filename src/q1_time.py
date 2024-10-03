from typing import List, Tuple
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format, row_number
from pyspark.sql.window import Window

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Se inicia la sesión de Pyspark
    spark = SparkSession.builder.appName("LatamChallenge").getOrCreate()
    # Se lee el archivo y se almacena en un dataframe 
    data = spark.read.json(file_path)

    # Se convierte la columna de date a fecha.
    data = data.withColumn('created_at_date', date_format(col('date'), 'yyyy-MM-dd'))
    # Se agrupa por fecha y usuario, contando cuántos tweets tiene cada usuario en cada fecha
    tweet_counts = data.groupBy('created_at_date', col('user.username').alias('username')).agg(count('*').alias('tweet_count'))
    # Se crea una ventana para clasificar a los usuarios por la cantidad de tweets en cada fecha
    window = Window.partitionBy('created_at_date').orderBy(col('tweet_count').desc())

    # Se añade una columna para obtener el ranking de tweets por fecha
    ranked_data = tweet_counts.withColumn('rank', row_number().over(window))

    # Filtrar para quedarnos solo con el usuario que tiene más tweets por cada fecha
    top_users_per_date = ranked_data.filter(col('rank') == 1).select('created_at_date', 'username')

    # Contar el total de tweets por fecha y ordenar para obtener las 10 fechas con más tweets
    top_dates = data.groupBy('created_at_date').agg(count('*').alias('total_tweets')).orderBy(col('total_tweets').desc()).limit(10)

    # Unir las tablas para obtener los usuarios correspondientes a las 10 fechas más activas
    top_result = top_dates.join(top_users_per_date, on='created_at_date', how='inner') \
                          .select('created_at_date', 'username')

    # Convertir el resultado a una lista de tuplas
    result_list = [(row['created_at_date'], row['username']) for row in top_result.collect()]

    return result_list 