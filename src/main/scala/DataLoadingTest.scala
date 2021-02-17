import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.apache.spark.sql.SparkSession


object DataLoadingTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .appName("DataLoadingTest")
//      .master("local[4]")
      .master("spark://Master:7077")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    val sc = spark.sparkContext

//    val csvFile = "D:\\projects\\data\\porto\\train.csv"
    val csvFile = "/datasets/porto_traj.csv"
    val geojsonFile = "/datasets/porto_taxi.geojson"

    val samplingRate = 15
    val numPartitions = 8

    loadCsv()
    loadCsvAndParse()

    sc.stop()

    def loadCsv(): Unit = {
      val df = spark.read.option("header", "true")
        .option("numPartitions", 8)
        .csv(csvFile)
      df.take(1)
      df.unpersist()
    }

    def loadCsvAndParse(): Unit = {
      val df = spark.read.option("header", "true")
        .option("numPartitions", numPartitions)
        .csv(csvFile)

      val trajRDD = df.rdd
        .repartition(numPartitions)
        .filter(row => row(8).toString.split(',').length >= 4) // each trajectory should have no less than 2 recorded points

      val resRDD = trajRDD.map(row => {
        val tripID = row(0).toString
        val taxiID = row(4).toString.toLong
        val startTime = row(5).toString.toLong
        val pointsString = row(8).toString
        var pointsCoord = new Array[Double](0)
        for (i <- pointsString.split(',')) pointsCoord = pointsCoord :+ i.replaceAll("[\\[\\]]", "").toDouble
        var points = new Array[geometry.Point](0)
        for (i <- 0 to pointsCoord.length - 2 by 2) {
          points = points :+ geometry.Point(Array(pointsCoord(i), pointsCoord(i + 1)), startTime + samplingRate * i / 2)
        }
        geometry.Trajectory(tripID, startTime, points, Map("taxiID" -> taxiID.toString))
      })

      resRDD.take(1)
      df.unpersist()
      trajRDD.unpersist()
      resRDD.unpersist()
    }


  }
}
