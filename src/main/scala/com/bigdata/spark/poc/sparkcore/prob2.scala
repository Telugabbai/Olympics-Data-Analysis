package com.bigdata.spark.poc.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object prob2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("prob2").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    // Here i'm working with problem statement 2
    // i.e Find the no.of medals that India won year wise

    val data="C:\\Users\\Sreenivas\\Documents\\olympics_data.csv"
    // creating RDD
    val RDD2=sc.textFile(data)
    //cleaning and computing the data
    val res=RDD2.map(x=>x.split(",")).filter(x=>{((x(2)=="India"))}).map(x=>(x(3),x(9).toInt)).reduceByKey(_+_)
    res.collect().foreach(println)
    //    res.take(20).foreach(println)
    // you can replace with country name what you want

    // saving data
    res.coalesce(2).saveAsTextFile("new1")
    spark.stop()
  }
}