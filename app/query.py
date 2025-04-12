from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log, sum as spark_sum, lit
import sys

# Initialize Spark with Cassandra support
spark = SparkSession.builder \
    .appName("CassandraBM25Calculator") \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .getOrCreate()

k1 = 1.2
b = 0.75
query_terms = sys.argv[1:]
print(f"Query: {query_terms}")

# Tables
document_frequency = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="document_frequency", keyspace="search_engine") \
    .load() \
    .select(col("word").alias("df_word"), col("count").alias("df_count"))
document_frequency.show(5)
# columns word->df_word, count->df_count

word_document_counts = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="word_document_counts", keyspace="search_engine") \
    .load() \
    .select(col("word").alias("wdc_word"), 
            col("document_id"), 
            col("count").alias("wdc_count"))
word_document_counts.show(5)
# columns word->wdc_word, document_id, count->wdc_count

doc_id_len = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="doc_id_len", keyspace="search_engine") \
    .load()
doc_id_len.show(5)
# document_id, length

N = doc_id_len.count()  # Total number of documents
avg_doc_length = doc_id_len.agg({"length": "avg"}).collect()[0][0]
print(N, avg_doc_length)

# Calculate IDF: log[(N - df + 0.5) / (df + 0.5) + 1]
query_df = document_frequency.filter(col("df_word").isin(query_terms))
idf = query_df.withColumn("idf", log((lit(N) - col("df_count") + 0.5) / (col("df_count") + 0.5) + 1)) \
              .select(col("df_word").alias("word"), "idf")
idf.show(5)

# Join with term frequencies and document lengths
bm25_components = word_document_counts \
    .filter(col("wdc_word").isin(query_terms)) \
    .join(doc_id_len, "document_id") \
    .join(idf, word_document_counts["wdc_word"] == idf["word"]) \
    .drop("word")  # Drop duplicate word column
bm25_components.show(5)

bm25_scores = bm25_components.withColumn(
    "bm25_component",
    col("idf") * (
        (col("wdc_count") * (k1 + 1)) / 
        (col("wdc_count") + k1 * (1 - b + b * (col("length") / avg_doc_length)))
    ))
bm25_scores.show(5)

# Sum BM25 scores per document
final_scores = bm25_scores.groupBy("document_id") \
    .agg(spark_sum("bm25_component").alias("bm25_score")) \
    .orderBy(col("bm25_score").desc())

print("\nTop documents for query:", " ".join(query_terms))
final_scores.show(10)

spark.stop()