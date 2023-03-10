import com.sensor.{SensorResult, SensorStatisticsUtil}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SensorTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _
  @transient var sensorResult: SensorResult = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("SensorSparkTest")
      .master("local[3]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sensorResult = new SensorStatisticsUtil().processSensorData("resources/", spark)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Test - Num of processed files") {
    val fileCount = sensorResult.noOfProcessedFile
    assert(fileCount == 2, " Num of processed files should be 2")
  }

  test("Test - Num of processed measurements") {
    val fileCount = sensorResult.numOfProcessedMeasure
    assert(fileCount == 7, " Num of processed measurements should be 7")
  }

  test("Test - Num of failed measurements") {
    val fileCount = sensorResult.numOfFailedMeasure
    assert(fileCount == 2, " Num of failed measurements should be 2")
  }

}
