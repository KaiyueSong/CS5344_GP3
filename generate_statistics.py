import findspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

findspark.init()

conf = SparkConf() \
    .set("spark.driver.memory", "25g") \
    .set("spark.executor.memory", "50g") \
    .set("spark.storage.level", "MEMORY_AND_DISK") \
    .set("spark.dynamicAllocation.enabled", "true") \
    .set("spark.shuffle.service.enabled", "true") \
    .set("spark.driver.maxResultSize", "5g") \
    .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
# .set("spark.executor.cores", "2") \
# .set("spark.executor.instances", "3") \

sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("RDD to DataFrame").getOrCreate()

########## Loading and processing the dataset ##########
csv_folder_path = "./similarity_results_v9"
df = spark.read.csv(csv_folder_path, header=False, inferSchema=True)

########## Selecting columns ##########
columns_to_select = ['date', 'title_desc', 'start_date', 'end_date', 'non_fake_date', 'non_fake_title_desc',
                     'cosine_similarity', 'euclidean_distance', 'manhattan_distance', 'jaccard_similarity',
                     'pearson_correlation']

df = df.toDF(*columns_to_select)


def calculate_statistics(df, column_name):
    stat_df = df.groupby('title_desc').agg(F.max(column_name).alias('stat_max'))
    stat = stat_df.agg(F.mean('stat_max'),
                       F.stddev('stat_max'),
                       F.count('stat_max')).collect()
    mean = stat[0][0]
    std = stat[0][1]
    count = stat[0][2]
    print(f"{column_name} mean: {mean}, std: {std}, count: {count}")


calculate_statistics(df, 'cosine_similarity')
calculate_statistics(df, 'euclidean_distance')
calculate_statistics(df, 'manhattan_distance')
calculate_statistics(df, 'jaccard_similarity')
calculate_statistics(df, 'pearson_correlation')
