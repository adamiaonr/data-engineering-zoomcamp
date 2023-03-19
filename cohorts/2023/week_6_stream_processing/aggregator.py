import os
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import SparkSession

RIDE_SCHEMA = T.StructType() \
        .add("pu_location_id", T.IntegerType()) \
        .add("do_location_id", T.IntegerType()) \
        .add("pu_datetime", T.StringType()) \
        .add("do_datetime", T.StringType()) \
        .add("color", T.StringType())

TOPIC_GREEN="rides_green"
TOPIC_FHV="rides_fhv"
TOPIC_ALL="rides_all"

def read_from_kafka(consume_topics: list[str]):
    """
    Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    """
    stream_data = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('BOOTSTRAP_SERVER')) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f"""org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.environ.get('CLUSTER_API_KEY')}" password="{os.environ.get('CLUSTER_API_SECRET')}";""") \
        .option("subscribe", ','.join(consume_topics)) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()

    return stream_data


def parse_kafka_message(data, schema):
    """
    Take a Spark Streaming dataframe, parses the value as JSON,
    assuming the passed schema
    """
    assert data.isStreaming is True, "DataFrame doesn't receive streaming data"

    return data.selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", schema).alias("data")) \
        .select("data.*")


def op_groupby_count(data, column_names:list[str]):
    """
    Applies groupBy() to streaming datafarme 
    and counts nr of elements per group
    """
    aggregated_data = data.groupBy(column_names).count()

    return aggregated_data


def preapre_kafka_data(data, value_columns, key_column=None):
    """
    Prepares data to be written to Kafka.
    Data in value is written in JSON format, with the fields passed in value_columns.
    """
    data = data.withColumn("value", F.to_json(F.struct(*[F.col(c) for c in value_columns])))
    if key_column:
        data = data.withColumnRenamed(key_column, "key")
        data = data.withColumn("key", data.key.cast('string'))

    return data.select(['key', 'value'])


def sink_console(data, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    """
    Writes stream data to console
    """
    write_query = data.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()

    return write_query


def sink_kafka(df, topic):
    write_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('BOOTSTRAP_SERVER')) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f"""org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.environ.get('CLUSTER_API_KEY')}" password="{os.environ.get('CLUSTER_API_SECRET')}";""") \
        .outputMode("complete") \
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint") \
        .start()

    return write_query

if __name__ == "__main__":
    spark = SparkSession.builder.appName('hw6').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # read two topics from kafka
    raw_data = read_from_kafka(consume_topics=[TOPIC_FHV, TOPIC_GREEN])
    # parse kafka messages from JSON according to schema
    parsed_data = parse_kafka_message(raw_data, RIDE_SCHEMA)
    # print parsed data to console
    sink_console(parsed_data, output_mode='append')

    # groupBy('pu_location_id') and count
    aggregated_data = op_groupby_count(parsed_data, ["pu_location_id"])
    # print sorted data to console
    sink_console(aggregated_data.sort(F.desc("count")))

    # prepare data to be written to kafka (in JSON format {'pu_location_id': ..., 'count': ...})
    kafka_data = preapre_kafka_data(
        aggregated_data.sort(F.desc("count")), 
        value_columns=['pu_location_id', 'count'],
        key_column='pu_location_id'
    )
    # publish to kafka
    sink_kafka(kafka_data, TOPIC_ALL)

    spark.streams.awaitAnyTermination()
