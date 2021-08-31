package com.epam.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ConvertCsvToParquet {

  def main(args: Array[String]): Unit = {
    val session : SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

//    val schema: StructType = StructType(Array(
//      StructField("id", StringType, true),
//      StructField("duration", StringType, true),
//      StructField("start_date", StringType, true),
//      StructField("start_station_name", StringType, true),
//      StructField("start_station_id", StringType, true),
//      StructField("end_date", StringType, true),
//      StructField("end_station_name", StringType, true),
//      StructField("end_station_id", StringType, true),
//      StructField("bike_id", StringType, true),
//      StructField("subscription_type", StringType, true),
//      StructField("zip_code", StringType, true)
//    ))

    val df = session.read.option("infernSchema", "true").option("header", "true").csv("/home/serha/POC Project/trip.csv")

    df.write.format("parquet").save("/home/serha/POC Project/trip")

  }

}
