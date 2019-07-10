package com.mmtechsoft.xmljson

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}

object ReadComplexJsonApplication {


  def main(args: Array[String]): Unit = {


    //val spark = SparkSession.builder.getOrCreate()

    val sparkConf = new SparkConf

    val spark = SparkSession.builder
      .config(sparkConf).master("local[*]")
      .getOrCreate()
    //    val schema = (new StructType).add("action", StringType).add("timestamp", TimestampType)


    val schema = (new StructType)
      .add("payload", (new StructType)
        .add("event", (new StructType)
          .add("action", StringType)
          .add("timestamp", LongType)
        )
      )


//    val schema = (new StructType).add("action", StringType).add("timestamp", StringType)
    //    val mdf = spark.read.option("multiline", "true")
    //      .json("C:\\Users\\Admin\\IdeaProjects\\welcome_scala_java\\src\\main\\scala\\com\\mmtechsoft\\xmljson\\time.json")
    val path = "C:\\Users\\Admin\\IdeaProjects\\welcome_scala_java\\src\\main\\scala\\com\\mmtechsoft\\xmljson\\time.json";
    //val df = spark.read.schema(schema).json(path).select($"payload.event.*")

    val df1 = spark.read.schema(schema).json(path)
    spark.read.schema(schema).json(path).select($"payload.event.*").show
//
//    df1.printSchema()
//    df1.show();
  }
}
