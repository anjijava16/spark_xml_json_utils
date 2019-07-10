package com.mmtechsoft.xmljson

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkBookJsonReadOperation {


  def main(args: Array[String]): Unit = {


    //val spark = SparkSession.builder.getOrCreate()

    val sparkConf = new SparkConf

    val spark = SparkSession.builder
      .config(sparkConf).master("local[*]")
      .getOrCreate()
    val mdf = spark.read.option("multiline", "true").json("C:\\Users\\Admin\\IdeaProjects\\welcome_scala_java\\src\\main\\scala\\com\\mmtechsoft\\xmljson\\book.json")

    mdf.printSchema()
    mdf.show();

  }
}
