package com.sensor

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

/**
 * Sensor Util class
 */
class SensorStatisticsUtil() {

  var fileList: List[File] = null
  var numOfProcessedMeasure = 0L
  var numOfFailedMeasure = 0L

  def processSensorData(dir: String, spark: SparkSession): SensorResult = {
    val noOfProcessedFile = numOfProcessedFiles(dir)
    val result = getResultDF(dir, spark)
    val resultDf = new SensorResult(noOfProcessedFile, numOfProcessedMeasure, numOfFailedMeasure, result)
    resultDf
  }

  /**
   * Method to find csv files count in given directory path
   * @param dir
   * @return
   */
  def numOfProcessedFiles(dir: String): Int = {
    val d = new File(dir)
    if (d.exists && d.isDirectory && d.canRead) {
      fileList = d.listFiles.filter(_.isFile).toList
    } else {
      println("Directory path is not valid!!! " + dir)
      return 0
    }
    fileList.size
  }

  /**
   *  read csv files as a dataframe to process the sensor data.
   * @param dir : Directory path
   * @param spark : SparkSession
   * @return : return dataframe result
   */
  def getResultDF(dir: String, spark: SparkSession): DataFrame = {
    val df = spark.read.option("header", true).csv(dir)
    numOfProcessedMeasure = df.count()
    numOfFailedMeasure = df.filter(row => row.getString(1) == "NaN").count()

    val faultySensor = df.groupBy("sensor-id").agg(min("humidity").alias("min_humidity")).where(col("min_humidity") === "NaN").select("sensor-id")
    faultySensor.createTempView("faultySensor")
    df.createTempView("allSensor")
    val validSensor = spark.sql("select `sensor-id`,min(humidity) as `min`,avg(humidity) as `avg`,max(humidity) as `max` from allSensor TB1 where TB1.`sensor-id` NOT IN (select distinct `sensor-id` from faultySensor) AND humidity!='NaN'  group by `sensor-id`")
    val inValidSensor = spark.sql("select `sensor-id`,'NaN' as `min`,'NaN' as `avg`,'NaN' as `max` from faultySensor")

    val finalSensorData = validSensor.unionAll(inValidSensor)
    finalSensorData
  }
}
