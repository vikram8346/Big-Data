import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object KMeans {
  type Point = (Double, Double)

  var centroids: Array[Point] = Array[Point]()

  def distance(x: Point, y: Point):
  Double = {
    var dist = Math.sqrt(Math.pow((x._1 - y._1), 2) + Math.pow((x._2 - y._2), 2))
    dist
  }

  //var dist: Double = new EuclideanDistance().compute(var x:Point, var y:Point);

  def main(args: Array[String]) {
    /* ... */
    val conf = new SparkConf().setAppName("project4run") /* TODO setAppName() is not mandatory, can be ignored */
    /*conf.setMaster("local[2]")*/
    val sc = new SparkContext(conf)

    //centroids = /* read initial centroids from centroids.txt */
    centroids = sc.textFile(args(1)).map(line => {val cl = line.split(",")
    (cl(0).toDouble, cl(1).toDouble)
    }).collect
    // TODO try using flatMap instead of map in above line
    // TODO args(1,0) may not be required, see example in lecture

    val points = sc.textFile(args(0)).map(line => {val pl = line.split(",")
    (pl(0).toDouble, pl(1).toDouble)
    }) /*TODO .collect required? */

    for (i <- 1 to 5) {
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p, _)), p) }.groupByKey().map { case (cntrd, pnts) =>
        var c:Int = 0
        var sx:Double = 0.0
        var sy:Double = 0.0
        /*points.foreach{*/
        for (eachp <- pnts) {
          c += 1
          sx += eachp._1
          sy += eachp._2
        }
        (sx / c, sy / c)
      }.collect
    }
    centroids.foreach(println)
    sc.stop()
  }
}
