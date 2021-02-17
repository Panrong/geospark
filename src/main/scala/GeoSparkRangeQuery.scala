import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, LineStringRDD, PointRDD, PolygonRDD}
import java.lang.System.nanoTime

object GeoSparkRangeQuery {
  def main(args: Array[String]): Unit = {
    // spark
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

//    val conf = new SparkConf().setAppName("GeoSparkRangeQuery").setMaster("local[*]")
    val conf = new SparkConf().setAppName("GeoSparkRangeQuery")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    val sc = new SparkContext(conf)

    // input data and parameters
    val dataFile = args(0)
    val allowTopologyInvalidGeometries = true // Optional for GeoJsonReader.readToGeometryRDD
    val skipSyntaxInvalidGeometries = false // Optional for GeoJsonReader.readToGeometryRDD
    val indexType = IndexType.RTREE

    // query
    val spatialRangeQuery  = new Envelope (-8.682329739182336,-8.553892156181982,
      41.16930767535641,41.17336956864337)
    //  val spatialRangeQuery  = new Envelope (-10,-6, 30, 50)
    val temporalRangeQuery = (1399900000L, 1400000000L)
    val eachQueryLoopTimes = 1

    // execution
    testSpatialRangeQuery()
    //  testSpatialTemporalRangeQuery()
    testSpatialRangeQueryUsingIndex()
    sc.stop()

    def testSpatialTemporalRangeQuery() {
      println("In function testSpatialTemporalRangeQuery: ")
      var t = nanoTime()
      val taxiRDD = GeoJsonReader.readToGeometryRDD(sc, dataFile, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
      taxiRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
      println(s"... Build LineStringRDD: ${(nanoTime() - t) * 1e-9} s.")

      // println(taxiRDD.rawSpatialRDD.count())

      t = nanoTime()
      val spatialQueryResult  = RangeQuery.SpatialRangeQuery(taxiRDD, spatialRangeQuery, true, false)
      println(spatialQueryResult.count())
      val rddWithOtherAttributes = spatialQueryResult.rdd.map[String](f => f.getUserData.asInstanceOf[String])
      rddWithOtherAttributes.take(5).filter(x => {
        //      println(x)
        val ts = x.split("\t")(7).toLong
        //      println(ts)
        ts <= temporalRangeQuery._2 && ts >= temporalRangeQuery._1
      })

      println(s"... Range query: ${(nanoTime() - t) * 1e-9 } s.")
    }

    def testSpatialRangeQuery() {
      println("In function testSpatialRangeQuery: ")
      var t = nanoTime()
      val taxiRDD = GeoJsonReader.readToGeometryRDD(sc, dataFile, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
      taxiRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
      println(s"... Build LineStringRDD: ${(nanoTime() - t) * 1e-9} s.")

      for (i <- 1 to eachQueryLoopTimes) {
        t = nanoTime()
        val spatialQueryResult  = RangeQuery.SpatialRangeQuery(taxiRDD, spatialRangeQuery, true, false)
        println(spatialQueryResult.count())
        println(s"... Range query: ${(nanoTime() - t) * 1e-9} s.")
      }

    }

    def testSpatialRangeQueryUsingIndex() {
      println("In function testSpatialRangeQueryUsingIndex: ")
      var t = nanoTime()
      val taxiRDD = GeoJsonReader.readToGeometryRDD(sc, dataFile, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
      taxiRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
      println(s"... Build LineStringRDD: ${(nanoTime() - t) * 1e-9} s.")

      t = nanoTime()
      taxiRDD.buildIndex(indexType, false)
      taxiRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
      println(s"... Build RTree index: ${(nanoTime() - t) * 1e-9} s.")

      t = nanoTime()
      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = RangeQuery.SpatialRangeQuery(taxiRDD, spatialRangeQuery, true, true).count
        println(resultSize)
      }
      println(s"... Range query with index: ${(nanoTime() - t) * 1e-9 / eachQueryLoopTimes} s.")

    }
  }
}
