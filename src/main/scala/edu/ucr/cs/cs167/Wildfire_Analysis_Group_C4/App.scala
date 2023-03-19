package edu.ucr.cs.cs167.Wildfire_Analysis_Group_C4

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.SparkSession


object App {

  def main(args: Array[String]): Unit = {





    val desiredCountyName = args(0)
    temporalAnalysis(desiredCountyName, "wildfiredb_ZIP.parquet")
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

      //Compute the total fire intensity, SUM(frp), and group by the combination of year and month.
      val desiredCountyIntensity = desiredCountyDF
        .groupBy("acq_date")
        .sum("frp")

      //Sort the results lexicographically by (year, month) in a csv file
      desiredCountyIntensity
        .sort("acq_date")
        .write
        .format("csv")
        .save("wildfires" + desiredCountyName + ".csv")
    }
  }
}