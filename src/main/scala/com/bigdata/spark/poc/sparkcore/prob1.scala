package com.bigdata.spark.poc.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object prob1 {
  def main(args: Array[String]) {

      val spark = SparkSession.builder.master(args(0)).appName(args(1)).getOrCreate()
      // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
      val sc = spark.sparkContext

      import spark.implicits._
      import spark.sql
      // Here i'm working with problem statement
      // i.e Find the total no.of medals won by each country in swimming
      val data = args(2)
      val op = "C:\\Users\\Sreenivas\\Documents\\newop"
      // creating RDD
      val RDD1 = sc.textFile(data)
      //cleaning the data
      val counts = RDD1.map {
        x => {
          (x.toString().split(","))
        }
      }
      // we are filtering swimming data only
      val swimmingdata = counts.filter(x => {
        ((x(5) == "Swimming"))
      })
      // pairing country column and total medals columns . no need swimming cloumn because total data based on swimming only
      val pairs = swimmingdata.map(x => (x(2), x(9).toInt))

      //computing
      val res = pairs.reduceByKey(_ + _)
      res.take(20).foreach(println)
      //    res.take(20).foreach(println)
      res.coalesce(2).saveAsTextFile(args(3))
    spark.stop()
  }
}