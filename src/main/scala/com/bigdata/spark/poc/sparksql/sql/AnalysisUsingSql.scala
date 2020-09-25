package com.bigdata.spark.poc.sparksql.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AnalysisUsingSql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("AnalysisUsingSql").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    // Athlete Dataframe creation
    val athlete_data="C:\\Users\\Sreenivas\\Documents\\Project\\Olympics\\Olympics-Data-Analysis\\target\\data\\athlete_events.csv"
    val athlete_df=spark.read.format("csv").option("header","true").option("inferSchema","true")
      .option("path",athlete_data).load()

    val athlete_df1=athlete_df.withColumn("ID", col("ID").cast("Double"))
      .withColumn("ID", col("ID").cast("Double"))
      .withColumn("Age", col("Age").cast("Double"))
      .withColumn("Height", col("Height").cast("Double"))
      .withColumn("Weight", col("Weight").cast("Double"))

    athlete_df1.createOrReplaceTempView("Sports")

    //NOC Dataframe Creation
    val noc_data="C:\\Users\\Sreenivas\\Documents\\Project\\Olympics\\Olympics-Data-Analysis\\target\\data\\noc_regions.csv"
    val noc_df=spark.read.format("csv").option("header","true").option("inferSchema","true")
      .option("path",noc_data).load()
    noc_df.createOrReplaceTempView("NOC")

//    Problem Statements:-
//
//    Find how many gold medels got based on age
    val pb1=spark.sql("select count(Medals) as Medals,Age from Sports where Medal='Gold' Group By Age Order By Age asc")
    pb1.show()
//    Gold Medals for Athletes Over 50 based on Sports
//      Women medals per edition(Summer Season) of the Games
//      Top 5 Gold Medal Countries
//      Disciplines with the greatest number of Gold Medals
//      Variation of Male Athletes over time
//      Variation of Female Athletes over time
//      Variation of Age for Male Athletes over time
//      Variation of Age for Female Athletes over time
//      Variation of Weight for Male Athletes over time
//      Variation of Weight for Female Athletes over time
//      Variation of Height for Male Athletes over time
//      Variation of Height for Female Athletes over time
//      Gold Medals based on Countries
//    Silver Medals based on Countries
//    Bronze Medals based on Countries
//    Total Medals won by India
//    Diff Medals won by India in Years wise










    spark.stop()
  }
}