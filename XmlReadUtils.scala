package com.mmtechsoft.xmljson


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object XmlReadUtils {


  def sparkContextDef(): SparkSession = {

    val sparkConf = new SparkConf
    val spark = SparkSession.builder
      .config(sparkConf).master("local[*]")
      .getOrCreate()

    return spark;
  }
  def main(args: Array[String]): Unit = {

    val spark = sparkContextDef();

    val df =spark.read.format("com.databricks.spark.xml")
       .option("rowTag", "book")
      .load("C:\\Users\\Admin\\IdeaProjects\\welcome_scala_java\\pom.xml");

    df.printSchema()

    df.show();




  }
}
