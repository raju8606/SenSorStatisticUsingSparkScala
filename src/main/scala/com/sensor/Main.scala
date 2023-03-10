package com.sensor

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Case class for Sensor results
 * @param noOfProcessedFile
 * @param numOfProcessedMeasure
 * @param numOfFailedMeasure
 * @param sensorData
 */
case class SensorResult(noOfProcessedFile: Int, numOfProcessedMeasure: Long, numOfFailedMeasure: Long, sensorData: DataFrame)

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Directory path is mandatory as a argument, Hence exiting the Application")
      System.exit(1)
    } else {
      println("Directory path is :" + args(0))
    }

    val dir = args(0)
    val sparkConf = new SparkConf().setAppName("Sensor Statistics Task").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /** Printing Sensor Result */
    val result = new SensorStatisticsUtil().processSensorData(dir, spark)
    println("Num of processed files: " + result.noOfProcessedFile)
    println("Num of processed measurements: " + result.numOfProcessedMeasure)
    println("Num of failed measurements: " + result.numOfFailedMeasure)
    println("Sensors with highest avg humidity:")
    result.sensorData.show()

    /** closing Spark Session */
    spark.close()

  }
}


