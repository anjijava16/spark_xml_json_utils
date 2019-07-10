package com.mmtechsoft.xmljson

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object ReadTimeJsonApplication {


  def main(args: Array[String]): Unit = {


    //val spark = SparkSession.builder.getOrCreate()

    val sparkConf = new SparkConf

    val spark = SparkSession.builder
      .config(sparkConf).master("local[*]")
      .getOrCreate()
//    val schema = (new StructType).add("action", StringType).add("timestamp", TimestampType)

    val schema = (new StructType).add("action", StringType).add("timestamp", StringType)
//    val mdf = spark.read.option("multiline", "true")
//      .json("C:\\Users\\Admin\\IdeaProjects\\welcome_scala_java\\src\\main\\scala\\com\\mmtechsoft\\xmljson\\time.json")

    val df = spark.read.schema(schema).json("C:\\Users\\Admin\\IdeaProjects\\welcome_scala_java\\src\\main\\scala\\com\\mmtechsoft\\xmljson\\time.json")
    //sqlContext.read.schema(schema).json(events).select($"action", $"timestamp".cast(LongType)).show
    df.printSchema()
    df.show();

  }
}
