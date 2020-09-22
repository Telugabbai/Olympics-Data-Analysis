package com.bigdata.spark.poc.sparksql

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._;


object OlympicsDataAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("OlympicsDataAnalysis").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    // Athlete Dataframe creation
    val athlete_data="C:\\Users\\Sreenivas\\Documents\\Project\\Olympics\\target\\data\\athlete_events.csv"
    val athlete_df=spark.read.format("csv").option("header","true").option("inferSchema","true")
        .option("path",athlete_data).load()
    val athlete_df1=athlete_df.withColumn("ID", col("ID").cast("Double"))
      .withColumn("ID", col("ID").cast("Double"))
      .withColumn("Age", col("Age").cast("Double"))
      .withColumn("Height", col("Height").cast("Double"))
      .withColumn("Weight", col("Weight").cast("Double"))

    //NOC Dataframe Creation
    val noc_data="C:\\Users\\Sreenivas\\Documents\\Project\\Olympics\\target\\data\\noc_regions.csv"
    val noc_df=spark.read.format("csv").option("header","true").option("inferSchema","true")
      .option("path",noc_data).load()

    /// Problem statement 1
    //Find how many gold medels got based on age


    val pb1=athlete_df1.select("Medal","Age")
      .filter(col("Medal")==="Gold").groupBy("Age").count().alias("Gold Medals")
      .orderBy("Age")
    pb1.show()

    // Problem Statement 2
    // Gold Medals for Athletes Over 50 based on Sports
    val pb2=athlete_df1.select("Sport","Age","Medal").where("Age > 50 and Medal == 'Gold'")
    pb2.show()


    //Problem Statement 3
    // Women medals per edition(Summer Season) of the Games
    val pb3=athlete_df1.select("Year","Medal","Season")
      .where(" Season == 'Summer' and Sex=='F' and  Medal in ('Gold','Silver','Bronze')")
      .groupBy("Year","Medal").count().alias("Medals").orderBy("Year")
    pb3.show()

    // Problem Statement 4
    //Top 5 Gold Medal Countries

    val pb4=athlete_df1 .join(noc_df,athlete_df1("NOC")===noc_df("NOC"),"inner")
      .select("region","Medal").where("Medal == 'Gold'")
      .groupBy("region","Medal").count().orderBy(col("Medal").asc).limit(5)
    pb4.show()

    // Problem Statement 5
    // Disciplines with the greatest number of Gold Medals

    val pb5=athlete_df1 .join(noc_df,athlete_df1("NOC")===noc_df("NOC"),"inner")
      .select("Event","Medal").where("Medal == 'Gold' and region == 'USA'")
      .groupBy("Event","Medal").count().orderBy(col("Medal").asc)
    pb5.show()
    // in spark SQL
    // val pb5=spark.sql("select count(Medal) as Medals, Event from athlete_df1 A JOIN noc_df N ON A.NOC = N.NOCwhere Medal = 'Gold' and A.NOC = 'USA' group by Event order by Medals desc")

    // Problem Statement 6
    //Variation of Male Athletes over time

    val pb6=athlete_df1.select("Year","Sex").where("Sex == 'M'")
      .groupBy("Year","Sex").count().alias("Males").orderBy("Year")
    pb6.show()

    // Problem Statement 7
    // Variation of Female Athletes over time
    val pb7=athlete_df1.select("Year","Sex").where("Sex == 'F'")
        .groupBy("Year","Sex").count().alias("Females").orderBy("Year")
    pb7.show()
    spark.stop()
  }
}