package com.sensor

trait SensorStatistics {

  def numOfProcessedFiles(dir: String): Int

  def numOfProcessedMeasurements(dir:String): Int

  def numOfFailedMeasurements():Int

}
