from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import explode, split
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws
import torch 
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification



spark = SparkSession.builder \
    .appName("BskySenti") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.jars", "//opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load()

df_string = df.selectExpr("CAST(value AS STRING) as json_string")


json_schema = StructType([
    StructField("text", StringType(), True)  # The JSON contains a "text" field
])


parsed_df = df_string.select(from_json(col("json_string"), json_schema).alias("data")) \
                     .select("data.text")


tokenizer = DistilBertTokenizer.from_pretrained('distilbert-base-uncased-finetuned-sst-2-english')
model = DistilBertForSequenceClassification.from_pretrained('distilbert-base-uncased-finetuned-sst-2-english')




def sentiment_analysis_udf(sentence):
    inputs = tokenizer(sentence, return_tensors="pt")
    with torch.no_grad():
        outputs = model(**inputs)
    logits = outputs.logits
    sentiments = torch.argmax(logits, dim=1).item()
    return "positive" if sentiments == 1 else "negative"


sentiment_udf = udf(sentiment_analysis_udf, StringType())

sentiment_df = parsed_df.withColumn("sentiment", sentiment_udf(parsed_df.text))

# output of sentimentdf is 
# +--------------------+---------+
# |                text|sentiment|
# +--------------------+---------+
# |I love you so much!| positive|
# |I hate you so much!| negative|
# +--------------------+---------+

# have to write this to postgresql, with columns text and sentiment respectively



jdbc_url = "jdbc:postgresql://localhost:5432/sentences"
connection_properties = {
    "user": "my_user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}


def write_to_postgres(df, id):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "sentiment_analysis") \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .mode("append") \
        .save()
    

query = sentiment_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()




