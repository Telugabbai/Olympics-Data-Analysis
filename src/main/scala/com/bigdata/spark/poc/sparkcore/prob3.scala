package com.bigdata.spark.poc.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object prob3 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("prob3").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data="C:\\Users\\Sreenivas\\Documents\\olympics_data.csv"
    // creating RDD
    val RDD3=sc.textFile(data)
    //cleaning and computing the data
    val res=RDD3.map(x=>(x.split(","))).map(x=>(x(2),x(9).toInt)).reduceByKey(_+_)
    res.collect().foreach(println)
    // saving data
    res.coalesce(2).saveAsTextFile("new2")

    spark.stop()
  }
}