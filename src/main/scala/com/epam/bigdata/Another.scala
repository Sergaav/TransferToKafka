package com.epam.bigdata

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileInputStream
import java.util.Properties

object Another {

  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("FromS3ToKafka")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val static: DataFrame = spark.read.format("parquet").load("/home/serha/POC Project/trip/*.parquet")

  val properties = new Properties()

  /**
   * Main method
   *
   * @param args args(0) = path to properties (by default ./config.properties)
   */
  def main(args: Array[String]): Unit = {
    val df = getDfFromLocalParquet("/home/serha/POC Project/trip/")

    insertToKafka(df)
  }

  def getDfFromLocalParquet(path: String): DataFrame = {
    spark
      .readStream
      .schema(static.schema)
      .parquet(path)
  }

  def transformDfToJson(df: DataFrame): DataFrame = {
    df.selectExpr("to_json(struct(*)) AS value")
  }

  /**
   * Inserts records from dataFrame to kafka topic
   *
   * @param dataFrame dataFrame to be pushed into topic
   */
  def insertToKafka(dataFrame: DataFrame): Unit = {
    transformDfToJson(dataFrame).writeStream
      .format("kafka")
      .option("checkpointLocation", "/home/serha/checkpoint1")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
      .option("topic", "test321")
      .start().awaitTermination()
  }

}
