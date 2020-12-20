import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
import scala.math.max


object Partition {

  val depth = 6
	var clstr_list = new ListBuffer[(Long, Long)] /*Todo Try with simple list or sequence*/
	var c = 0

  def main ( args: Array[ String ] ){

    val conf = new SparkConf().setAppName("project5")
    val sc = new SparkContext(conf)



    var graph = sc.textFile(args(0)).map(cl => cl.split(",")).map(cl =>
    			if (c<5) {
    				c+=1
						//(cl(0).toLong, cl(1).toLong, cl.drop(2).toList)
            (cl(0).toLong, cl(0).toLong, cl.drop(1).toList)
    			} else {
    				c+=1
						//(cl(0).toLong, -1, cl.drop(2).toList)
            (cl(0).toLong, -1, cl.drop(1).toList) /*Todo changes required*/
    			})

    for (i <- 1 to depth) {
      graph = graph.flatMap{case (node_id, cl_id, adj_vert) => clstr_list += ((node_id, cl_id))
			val long_vrtcs = adj_vert.map(_.toLong)
			if (cl_id > -1)
			{
  			for (each <- adj_vert)
  				clstr_list += ((each, cl_id))   /*Todo changes required*/
			}
//			else {
//				return None
//			}
				clstr_list  /*Todo Remove this, try collect*/
  		}
		.reduceByKey(_ max _)
		.join(graph.map(ng =>(ng._1, (ng._2, ng._3))))
		.map(ng =>{ var new_graph=(ng._1, ng._2._1, ng._2._2._2)
      		if ( ng._2._2._1 == -1)
        		new_graph = (ng._1, ng._2._1, ng._2._2._2)
		
      		else
			new_graph = (ng._1, ng._2._2._1, ng._2._2._2)

	new_graph
    	})//.collect
    }
val partition size = graph.map(ng => (ng._2, 1))
			.reduceByKey(_ + _)
			.collect()
			.foreach(println)

  }
}

