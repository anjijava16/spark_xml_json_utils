package com.mmtechsoft.xmljson

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import com.databricks.spark.xml._
import org.apache.spark.SparkConf


object SparkBookXmlReadOperation {


  def main(args: Array[String]): Unit = {


    //val spark = SparkSession.builder.getOrCreate()

    val sparkConf = new SparkConf

    val spark = SparkSession.builder
      .config(sparkConf).master("local[*]")
      .getOrCreate()
    val customSchema = StructType(Array(
      StructField("_id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true)))


    val df = spark.read
      .option("rowTag", "book")
      .schema(customSchema)
      .xml("C:\\Users\\Admin\\IdeaProjects\\welcome_scala_java\\src\\main\\scala\\com\\mmtechsoft\\xmljson\\books.xml")

    df.printSchema()
    df.show();
  }
}
