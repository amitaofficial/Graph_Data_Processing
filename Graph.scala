import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {

   def getGroupId(vid:Long,new_groups:RDD[(Long,Long)]) : Long = {
         return new_groups.filter{case(x,y)=>x==vid}.first._2
   }
    
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    // A graph is a dataset of vertices, where each vertex is a triple
    //   (group,id,adj) where id is the vertex id, group is the group id
    //   (initially equal to id), and adj is the list of outgoing neighbors
    var graph: RDD[ ( Long, Long, List[Long] ) ]
       = sc.textFile(args(0)).map(line=>{val tokens = line.split(",",2) // read the graph from the file args(0)
                                                var adjacent = List[Long]()
                                                if(tokens.length > 1){ adjacent = tokens(1).split(",").map(_.toLong).toList} // to avoid cases when there are no adjacent nodes
                                                val vertexid = tokens(0).toLong
                                          (vertexid,vertexid,adjacent)})     


    for ( i <- 1 to 5 ){
         // For each vertex (group,id,adj) generate the candidate (id,group)
       //    and for each x in adj generate the candidate (x,group).
       // Then for each vertex, its new group number is the minimum candidate

        val v_groups = graph.map{case (group,vid,adj)=> (vid,group)} // (id,group) candidate
        val adj_groups = graph.flatMap{case (group,vid,adj)=> adj.map((adj_vid)=>(adj_vid,group))} //(x,group) candidate
        val groups:RDD[(Long, Long)] = v_groups.union(adj_groups) //combining all tuples
        val new_groups = groups.groupByKey().mapValues(_.iterator.min) // group by vertex id and find minimum group number for each vertexid
        

        val pair_graph = graph.map(x=>(x._2,x)) // map graph with key as vertex id 
        val pair_groups = new_groups.map(x=>(x._1,x)) // map new group numbers with key as vertexid

        graph = pair_graph.join(pair_groups).map{case (k,(graph,group))=> (group._2,group._1,graph._3 )} // do join and reconstruct the graph using the new group numbers. graph_.3 gives the adjacent vertices for a vertex



    }

    val group_size = graph.map{case (group,vid,adj) => (group,1)}.reduceByKey(_+_) //calculate group size

    group_size.collect().foreach(println)
    print("************************************************************************************************ output above ************************************************************************************************") // printed for easy identification in the output


    sc.stop()
  }
}
