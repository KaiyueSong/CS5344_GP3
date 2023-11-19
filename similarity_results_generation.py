import gc
import re

import findspark
import nltk
import numpy as np
from nltk import PorterStemmer
from pyspark import SparkConf, SparkContext
from pyspark.ml.feature import Word2Vec, Word2VecModel
from pyspark.ml.linalg import DenseVector
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, date_sub, date_add, col

findspark.init()
# Setting up Spark and nltk configurations
nltk.download('punkt')
nltk.download('stopwords')

conf = SparkConf() \
    .set("spark.driver.memory", "25g") \
    .set("spark.executor.memory", "50g") \
    .set("spark.storage.level", "MEMORY_AND_DISK") \
    .set("spark.dynamicAllocation.enabled", "true") \
    .set("spark.shuffle.service.enabled", "true") \
    .set("spark.driver.maxResultSize", "5g") \
    .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
# .set("spark.executor.cores", "2") \
# .set("spark.executor.instances", "3")\

sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("RDD to DataFrame").getOrCreate()

stemmer = PorterStemmer()
stopwords = nltk.corpus.stopwords.words('english')

# Loading and processing the dataset
input_file = './similarity_data_new_date.tsv'
columns_to_select = ['date', 'title', 'desc', 'category', 'DocumentIdentifier']
fake_model_path = './fake_model_word2vec'
non_fake_model_path = './non_fake_model_word2vec'
all_word_2_vec_model_path = './all_word_2_vec_model'

lines = sc.textFile(input_file)
rdd_split = lines.map(lambda l: l.split("\t"))
header = rdd_split.first()
rdd_split_without_header = rdd_split.filter(lambda l: l != header)

print(rdd_split_without_header.take(1))

indices_to_select = [header.index(col) for col in columns_to_select]
selected_rdd = rdd_split.map(lambda fields: [fields[i] for i in indices_to_select])
selected_rdd_header = selected_rdd.first()

title_index = selected_rdd_header.index('title')
desc_index = selected_rdd_header.index('desc')

print("im here")


def join_title_description(fields):
    title = fields[title_index]
    desc = fields[desc_index]
    if title is None:
        title = ""
    if desc is None:
        desc = ""

    title_description = title + " " + desc
    fields.append(title_description)
    return fields


joined_rdd = selected_rdd.map(join_title_description)
header = selected_rdd_header + ['title_desc']
title_desc_index = header.index('title_desc')
date_index = header.index('date')
category_index = header.index('category')


def preprocess_content(l):
    l[title_desc_index] = l[title_desc_index].strip()
    l[title_desc_index] = l[title_desc_index].lower()
    l[title_desc_index] = l[title_desc_index].replace('\n', ' ')
    l[title_desc_index] = l[title_desc_index].replace('\t', ' ')
    l[title_desc_index] = l[title_desc_index].replace(',', ' ')
    l[title_desc_index] = l[title_desc_index].replace('.', ' ')
    l[title_desc_index] = re.sub(' +', ' ', l[title_desc_index])

    # tokens = word_tokenize(l[title_desc_index])
    # filtered_tokens = [stemmer.stem(token) for token in tokens if token.lower() not in stopwords]
    # processed_sentence = ' '.join(filtered_tokens)
    # l[title_desc_index] = processed_sentence

    return l


preprocessed_rdd = joined_rdd.map(preprocess_content)

fake_rdd = preprocessed_rdd.filter(lambda fields: fields[category_index] == "fake")
non_fake_rdd = preprocessed_rdd.filter(lambda fields: fields[category_index] == "non-fake")

print(fake_rdd.take(1))

fake_df = spark.createDataFrame(fake_rdd.map(lambda x: (x[date_index], x[title_desc_index])), ['date', "title_desc"])
non_fake_df = spark.createDataFrame(non_fake_rdd.map(lambda x: (x[date_index], x[title_desc_index])),
                                    ['date', "title_desc"])
# non_fake_df = non_fake_df.limit(10000)

fake_df = fake_df.withColumn("words", split(fake_df["title_desc"], " "))
non_fake_df = non_fake_df.withColumn("words", split(non_fake_df["title_desc"], " "))

print("im here before processing word2vec")
word2Vec = Word2Vec(vectorSize=100, minCount=5, inputCol="words", outputCol="result")

# fake_model = word2Vec.fit(fake_df)
# fake_model.save(fake_model_path)
# fake_model = None
# gc.collect()

combined_df = fake_df.union(non_fake_df)

# Initialize the Word2Vec model
word2Vec = Word2Vec(vectorSize=100, minCount=5, inputCol="words", outputCol="result")

# Train the Word2Vec model on the combined dataset
model_train = False
if model_train:
    combined_model = word2Vec.fit(combined_df)
    combined_model.save(all_word_2_vec_model_path)
else:
    combined_model = Word2VecModel.load(all_word_2_vec_model_path)
combined_wordVectors = combined_model.transform(combined_df)
print("im here after processing word2vec")

print("im here after processing fake word vectors")
# non_fake_model = word2Vec.fit(non_fake_df)
# non_fake_model.save(non_fake_model_path)
# non_fake_model = None
# combined_model = None
gc.collect()
print("im here after processing non fake model")

# Broadcast the fake_wordVectors to every worker
fake_wordVectors = combined_model.transform(fake_df)
non_fake_wordVectors = combined_model.transform(non_fake_df)

fake_wordVectors = fake_wordVectors.withColumnRenamed("result", "fake_result")
non_fake_wordVectors = non_fake_wordVectors.withColumnRenamed("result", "non_fake_result")

from pyspark.sql.functions import to_timestamp

fake_wordVectors = fake_wordVectors.withColumn("date", to_timestamp(fake_wordVectors["date"], "yyyy-MM-dd HH:mm:ss z"))
non_fake_wordVectors = non_fake_wordVectors.withColumn("date", to_timestamp(non_fake_wordVectors["date"],
                                                                            "yyyy-MM-dd HH:mm:ss z"))

fake_wordVectors = fake_wordVectors.withColumn("start_date", date_sub(col("date"), 3))
fake_wordVectors = fake_wordVectors.withColumn("end_date", date_add(col("date"), 2))

fake_wordVectors = fake_wordVectors.withColumnRenamed("result", "fake_result")
non_fake_wordVectors = non_fake_wordVectors.withColumnRenamed("result", "non_fake_result")

non_fake_wordVectors = non_fake_wordVectors.withColumnRenamed("date", "non_fake_date")
non_fake_wordVectors = non_fake_wordVectors.withColumnRenamed("title_desc", "non_fake_title_desc")
non_fake_wordVectors = non_fake_wordVectors.withColumnRenamed("words", "non_fake_words")

joined_df = fake_wordVectors.join(non_fake_wordVectors,
                                  (non_fake_wordVectors.non_fake_date >= fake_wordVectors.start_date) &
                                  (non_fake_wordVectors.non_fake_date <= fake_wordVectors.end_date))


def cosine_similarity(v1, v2):
    return float(v1.dot(v2) / (v1.norm(2) * v2.norm(2)))


from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType


def compute_cosine_similarity(v1, v2):
    return float(v1.dot(v2) / (v1.norm(2) * v2.norm(2)))


def compute_euclidean_distance(v1, v2):
    # Convert DenseVector to NumPy array if needed
    if isinstance(v1, DenseVector):
        v1 = v1.toArray()
    if isinstance(v2, DenseVector):
        v2 = v2.toArray()

    # Now perform the operation with NumPy arrays
    return float(np.sqrt(np.sum((v1 - v2) ** 2)))


# Function to compute Manhattan distance
def compute_manhattan_distance(v1, v2):
    return float(np.sum(np.abs(v1 - v2)))


def jaccard_similarity(str1, str2):
    set1 = set(str1)  # Splitting the string into words
    set2 = set(str2)
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return float(intersection) / float(union) if union != 0 else 0


# Function to compute Pearson correlation between two vectors
def compute_pearson_correlation(v1, v2):
    v1, v2 = np.array(v1), np.array(v2)
    if v1.size == v2.size:
        # Compute correlation
        mean_v1, mean_v2 = np.mean(v1), np.mean(v2)
        numerator = np.sum((v1 - mean_v1) * (v2 - mean_v2))
        denominator = np.sqrt(np.sum((v1 - mean_v1) ** 2) * np.sum((v2 - mean_v2) ** 2))
        if denominator == 0:
            return None  # Handle division by zero
        return float(numerator / denominator)
    else:
        return None  # Or handle unequal length vectors as needed


# Registering the functions as UDFs
euclidean_distance_udf = udf(compute_euclidean_distance, FloatType())
manhattan_distance_udf = udf(compute_manhattan_distance, FloatType())
cosine_similarity_udf = udf(compute_cosine_similarity, FloatType())
jaccard_similarity_udf = udf(jaccard_similarity, FloatType())
pearson_correlation_udf = udf(compute_pearson_correlation, FloatType())

# Using the UDFs in DataFrame operations
result_df = joined_df.withColumn("jaccard_similarity",
                                 jaccard_similarity_udf(joined_df.words, joined_df.non_fake_words))
print("jacard similarity done")
result_df = result_df.withColumn("pearson_correlation",
                                 pearson_correlation_udf(joined_df.fake_result, joined_df.non_fake_result))
print("pearson correlation done")
result_df = result_df.withColumn("cosine_similarity",
                                 cosine_similarity_udf(joined_df.fake_result, joined_df.non_fake_result))
print("cosine similarity done")
result_df = result_df.withColumn("euclidean_distance",
                                 euclidean_distance_udf(joined_df.fake_result, joined_df.non_fake_result))
print("euclidean distance done")
result_df = result_df.withColumn("manhattan_distance",
                                 manhattan_distance_udf(joined_df.fake_result, joined_df.non_fake_result))
print("manhattan distance done")

print(result_df.columns)
columns_to_select = ['date', 'title_desc', 'start_date', 'end_date', 'non_fake_date', 'non_fake_title_desc',
                     'cosine_similarity', 'euclidean_distance', 'manhattan_distance', 'jaccard_similarity',
                     'pearson_correlation']

result_df = result_df.select(columns_to_select)
print("im here after selecting columns")
# Save the results to a CSV
result_df.write.csv('similarity_results_v9.csv')
print("im here after saving to csv")