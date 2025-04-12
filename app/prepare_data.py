from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

# use same path as in prepare_data.sh line 15
df = spark.read.parquet("/data/a.parquet")
n = 1000

df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)
def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(str(row['id'])+' '+row['text'])

# save data, will not be modified
df.foreach(create_doc)

# prepare data for modifications
df.write.csv("/index/data", sep = "\t")

# Download stopwords
import requests
stopwords_list = requests.get("https://gist.githubusercontent.com/rg089/35e00abf8941d72d419224cfd5b5925d/raw/12d899b70156fd0041fa9778d657330b024b959c/stopwords.txt").content
stopwords = stopwords_list.decode().splitlines()
stopwords_txt='\n'.join(word for word in stopwords)
with open("stopwords.txt", "w") as text_file:
    text_file.write(stopwords_txt)