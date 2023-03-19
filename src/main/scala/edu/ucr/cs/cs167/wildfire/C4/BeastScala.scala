package edu.ucr.cs.cs167.wildfire.C4

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "convert" =>
          val outputFile = args(2)
          // Sample program arguments: count-by-county Tweets_1k.tsv

          //Parse the CSV File
          val wildfireRDD: SpatialRDD = sparkContext.readCSVPoint(inputFile,"x","y",'\t')
          val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")

          //Do Spatial Join
          val wildfireCountyRDD: RDD[(IFeature, IFeature)] = wildfireRDD.spatialJoin(countiesRDD)

          //Create the County Column
          val wildfireCounty: DataFrame = wildfireCountyRDD.map({ case (wildfire, county) => Feature.append(wildfire, county.getAs[String]("GEOID"), "County") })
            .toDataFrame(sparkSession)

          //Convert the RDD to a Dataframe
          val convertedDF: DataFrame = wildfireCounty.selectExpr("acq_date", "double(split(frp,',')[0]) as frp", "acq_time", "County")

          //Write the output to parquet
          convertedDF.write.mode(SaveMode.Overwrite).parquet(outputFile)

        case _ => validOperation = false
      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}