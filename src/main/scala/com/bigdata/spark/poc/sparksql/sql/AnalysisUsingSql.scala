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
    val pb1=spark.sql("select count(Medal) as Medals,Age from Sports where Medal='Gold' Group By Age Order By Age asc")
    pb1.show()

//    Gold Medals for Athletes Over 50 based on Sports

    val pb2=spark.sql("select count(Medal) as Medals,Sport,Age from Sports where Medal='Gold'  and Age > 50 Group By Age,Sport Order By Age asc ")
    pb2.show()

//      Women medals per edition(Summer Season) of the Games

    val pb3=spark.sql("select Year,count(Medal) as Medals from Sports where Sex='F' and Season='Summer' and Medal in('Gold','Silver','Bronze') group by Year order by Year asc ")
      pb3.show()

//      Top 5 Gold Medal Countries

    val pb4=spark.sql("select count(S.Medal)as Medals,N.region from Sports S JOIN NOC N ON S.NOC=N.NOC where S.Medal ='Gold' group by region order by Medals desc limit 5")
    pb4.show()

//      Disciplines with the greatest number of Gold Medals

    val pb5=spark.sql("select Event,count(Medal) as Medals from Sports where NOC='USA' and Medal='Gold' group by Event order by Medals desc ")
    pb5.show()

//      Variation of Male Athletes over time

    val pb6=spark.sql("select count(Sex) as Males,Year from Sports where Sex='M' group by Year order by Year asc")
    pb6.show()

//      Variation of Female Athletes over time

      val pb7=spark.sql("select count(Sex) as Females,Year from Sports where Sex='F' group by Year order by Year asc")
        pb7.show()

//      Variation of Age for Male Athletes over time

    val pb8=spark.sql("select Min(Age),Max(Age),Avg(Age),Year from Sports where Sex='M' group by Year order by Year asc")
    pb8.show()

//      Variation of Age for Female Athletes over time

    val pb9=spark.sql("select Min(Age),Max(Age),Avg(Age),Year from Sports where Sex='F' group by Year order by Year asc")
      pb9.show()

//      Variation of Weight for Male Athletes over time

    val pb10=spark.sql("select Min(Weight),Max(Weight),Avg(Weight),Year from Sports where Sex='M' group by Year order by Year asc")
      pb10.show()

//      Variation of Weight for Female Athletes over time

      val pb11=spark.sql("select Min(Weight),Max(Weight),Avg(Weight),Year from Sports where Sex='F' group by Year order by Year asc")
        pb11.show()

//      Variation of Height for Male Athletes over time

      val pb12=spark.sql("select Min(Height),Max(Height),Avg(Height),Year from Sports where Sex='M' group by Year order by Year asc")
         pb12.show()

//      Variation of Height for Female Athletes over time

      val pb13=spark.sql("select Min(Height),Max(Height),Avg(Height),Year from Sports where Sex='F' group by Year order by Year asc")
         pb13.show()

//      Gold Medals based on Countries

    val pb14=spark.sql("select N.region,count(S.Medal) as Gold_Medals from Sports S JOIN NOC N ON S.NOC=N.NOC where S.Medal ='Gold' group by region order by Gold_Medals asc")
      pb14.show()

//    Silver Medals based on Countries

      val pb15=spark.sql("select N.region,count(S.Medal) as Silver_Medals from Sports S JOIN NOC N ON S.NOC=N.NOC where S.Medal ='Silver' group by region order by Silver_Medals asc")
       pb15.show()

//    Bronze Medals based on Countries

      val pb16=spark.sql("select N.region,count(S.Medal) as Bronze_Medals from Sports S JOIN NOC N ON S.NOC=N.NOC where S.Medal ='Bronze' group by region order by Bronze_Medals asc")
          pb16.show()

//    Total Medals won by India

    val pb17=spark.sql("select S.Year,count(S.Medal) as Medals from Sports S JOIN NOC N ON S.NOC=N.NOC where region='India' group by Year order by Medals asc")
     pb17.show()
    pb17.coalesce(1)
    pb17.write.csv("India")

//    Total Gold Medals won by Israel in Years wise
    val pb18=spark.sql("select S.Year,count(S.Medal) as Gold_Medals from Sports S JOIN NOC N ON S.NOC=N.NOC where N.region='Israel' and S.Medal='Gold' group by Year order by Gold_Medals asc")
      pb18.show()

    spark.stop()
  }
}
