package com.epam.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes.{StringType, TimestampType}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class TransferToKafkaTest extends AnyFlatSpec with Matchers {


  it should "TransferToKafka.readParquetFromS3" in {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val path = "/home/serha/IdeaProjects/TransferToKafka/*.parquet"
    val expectedDF = session.read.format("parquet").load(path)
    val readDF = TransferToKafka.readParquetFromS3(path, session)
    session.streams.awaitAnyTermination(100)
    readDF.schema shouldEqual (expectedDF.schema)
  }

  it should "TransferToKafka.writeDataToKafka" in {
    val path = "/home/serha/IdeaProjects/TransferToKafka/*.csv"
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val schema = session.read.option("infernSchema", "true").csv(path).schema
    val df = session
      .readStream
      .schema(schema)
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .format("csv").load(path)

    TransferToKafka.writeDataToKafka(df, "test-topic")
    session.streams.awaitAnyTermination(100)

    val readDF = session.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
      .option("subscribe", "test-topic")
      .load()

    readDF.columns.length shouldBe(7)
  }


}