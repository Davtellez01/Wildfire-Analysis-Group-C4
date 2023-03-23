package edu.ucr.cs.cs167.Wildfire_Analysis_Group_C4

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, format_string, lit, month, year}


object App {

  def main(args: Array[String]): Unit = {





    val desiredCountyName = args(0)
    temporalAnalysis(desiredCountyName, "wildfiredb10k_ZIP")
    }
  def temporalAnalysis(countyName : String, wildFireFileName : String): Unit = {
    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val desiredCountyName = countyName
    val wildFireFile: String = "wildfiredb_ZIP.parquet"
    //    val wildFireFile: String = wildFireFileName


    try {
      // Import Beast features\
      import edu.ucr.cs.bdlab.beast._

      //setup dataframe/RDD
      val wildFireDF = sparkSession.read.format("parquet")
        .load(wildFireFile)
      val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")

      //find the county that matches the desired county name and STATEFP="06"
      val desiredCounty = countiesRDD.filter(county =>
        county.getAs[String]("NAME") == desiredCountyName &&
          county.getAs[String]("STATEFP") == "06")
        .first()

      //get the county ID
      val desiredCountyID = desiredCounty.getAs[String]("GEOID")

      // get all entries in the wildfire database that match the desired county ID
      val desiredCountyDF = wildFireDF.filter(wildFireDF("County") === desiredCountyID)

      //Compute the total fire intensity, SUM(frp), and group by year and month but not day
      val desiredCountyIntensity = desiredCountyDF
        .groupBy(concat(
          year(desiredCountyDF("acq_date")),
          lit("-"),
          format_string("%02d", month(desiredCountyDF("acq_date"))))
          .as("year_month")
        )
        .sum("frp")

      //Sort the results lexicographically by (year, month) in a csv file
      desiredCountyIntensity
        .sort("year_month")
        .write
        .format("csv")
        .save("wildfires" + desiredCountyName + ".csv")
    }
  }
}