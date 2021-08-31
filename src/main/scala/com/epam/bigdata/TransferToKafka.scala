package com.epam.bigdata

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object TransferToKafka {

  def main(args: Array[String]): Unit = {
    val inputPath = if (args(0).nonEmpty) args(0) else return
    val topic = if (args(1).nonEmpty) args(1) else return
    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("TransferDataToKafka")
      .getOrCreate()

    writeDataToKafka(readParquetFromS3(inputPath, sparkSession), topic)

    sparkSession.streams.awaitAnyTermination()
  }

  def writeDataToKafka(dataset: DataFrame, topic: String): Unit = {
    dataset
      .select(to_json(struct("*")).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.enable.idempotence", "true")
      .option("topic", topic)
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
      .option("checkpointLocation", "/home/serha/checkPointKafka")
      .start
  }

  def readParquetFromS3(path: String, session: SparkSession): DataFrame = {
    val static = session.read.format("parquet").load(path)
    session
      .readStream
      .schema(static.schema)
      .option("maxFilesPerTrigger", 1)
      .parquet(path)
  }
}
