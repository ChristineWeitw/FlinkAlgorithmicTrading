import logging

from cassandra.cluster import Cluster   # pip install cassandra-driver
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, MapType, DoubleType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS stock_news
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS stock_news.properties (
            author text,
            content text,
            created_at text,
            headline text,
            id bigint,
            source text,
            summary text,
            updated_at text,
            url text,
            timestamp text,
            data_provider text,
            sentiment double,
            symbol text,
            PRIMARY KEY (id)
        );
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("Inserting data...")
    session.execute("""
        INSERT INTO stock_news.properties(author, content, created_at, headline, id, source, summary, 
                    updated_at, url, timestamp, data_provider, sentiment, symbol)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(kwargs.values()))

    print("Data inserted successfully!")


def create_cassandra_session():
    session = Cluster(["localhost"]).connect()

    if session is not None:
        create_keyspace(session)
        create_table(session)

    return session
def main():
    logging.basicConfig(level=logging.INFO)

    spark = (SparkSession.builder.appName("Consumer")
            #  .config("spark.cassandra.connection.host", "localhost")   
             .config("spark.jar.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
             .getOrCreate()
             )

    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "market-news")  # topic name
                .option("startingOffsets", "earliest")
                .load())
    
    schema = StructType([
        StructField("author", StringType(), True),
        StructField("content", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("id", LongType(), True),
        StructField("images", ArrayType(MapType(StringType(), StringType())), True),
        StructField("source", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("url", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("timestamp_ms", LongType(), True),
        StructField("data_provider", StringType(), True),
        StructField("sentiment", DoubleType(), True),
        StructField("symbol", StringType(), True)
    ])

    kafka_df = (kafka_df.selectExpr("CAST(value AS STRING) as value")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.author", "data.content", "data.created_at", "data.headline", "data.id", 
                        "data.source", "data.summary", "data.updated_at", "data.url", 
                        "data.timestamp", "data.data_provider", "data.sentiment", "data.symbol")
                )

    cassandra_query = (kafka_df.writeStream
                       .foreachBatch(lambda batch_df, batch_id: batch_df.foreach(
        lambda row: insert_data(create_cassandra_session(), **row.asDict())))
                       .start()
                       .awaitTermination()
                       )

    
if __name__ == "__main__":
    main()