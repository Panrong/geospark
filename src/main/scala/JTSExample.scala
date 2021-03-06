import com.vividsolutions.jts.geom.{Coordinate, Envelope}

object JTSExample extends App {

  val queryWindow  = new Envelope (-8.682329739182336,-8.553892156181982,
                                    41.16930767535641,41.17336956864337)


  val points = Array((-8.600112, 41.182704), (-8.599743, 41.182704), (-8.59797, 41.182236), (-8.597853, 41.182191), (-8.597853, 41.182182), (-8.597844, 41.182173), (-8.597835, 41.182173), (-8.597826, 41.182164), (-8.596692, 41.182056), (-8.59437, 41.181354), (-8.591373, 41.180121), (-8.588907, 41.179608), (-8.586936, 41.179617), (-8.585649, 41.180364), (-8.583822, 41.181273), (-8.583651, 41.181201), (-8.58366, 41.181183), (-8.58366, 41.181192), (-8.583588, 41.181156), (-8.582373, 41.180526), (-8.581644, 41.180094), (-8.581014, 41.179257), (-8.580105, 41.178789), (-8.580357, 41.178474))
  for (p <- points) {
    val coord = new Coordinate(p._1, p._2)
    print(p)
    println(queryWindow.contains(coord))
  }


}
