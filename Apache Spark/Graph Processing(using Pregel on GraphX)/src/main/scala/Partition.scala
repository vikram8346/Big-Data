import org.apache.spark.graphx.{Graph, VertexId, Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Partition {
  def main ( args: Array[String] ) {
  	val conf = new SparkConf().setAppName("FinalProject")
  	val sc = new SparkContext(conf)
  	var c=0
        var evrtx: Long = 0

/*Read the input graph and construct the RDD of edges*/

  	val rddedge = sc.textFile(args(0)).map( line => { val (nd, adjvrt) = line.split(",").splitAt(1)
    (nd(0).toLong, adjvrt.toList.map(_.toLong))})
    .flatMap( nn => nn._2.map(nadj => (nn._1, nadj)))
    .map(fn => Edge(fn._1, fn._2, 0L))
  	

/*Use the graph builder Graph.fromEdges to construct a Graph from the RDD of edges*/        
/*Access the VertexRDD and change the value of each vertex to be the -1 except for the first 5 nodes*/

        val initialGraph = Graph.fromEdges(rddedge, 0L).mapVertices((id,attr)=> {
          
  		if(c<5){
  		    c+=1
  		    evrtx=id
  		}
                else{
                  evrtx = -1
                }
  		evrtx
        })

  	

        val sssp = initialGraph.pregel(-1L, 5)(
  	(id, cl, mdata) => {
          if (cl == -1){

          math.max(cl, mdata)}
          
          else{
            cl
          }
  }, 	


        triplet=> {
  		if(triplet.dstAttr == -1L){
  			Iterator((triplet.dstId, triplet.srcAttr))
  		}
  		else{
  			Iterator.empty
  		}
  	},


  	(a,b)=>math.max(a, b))    //Merge Message
  	

        var ps = sssp.vertices.map(ng => (ng._2, 1))
        .reduceByKey(_ + _)
	ps.collect()
  	.foreach(println)

  }
}
