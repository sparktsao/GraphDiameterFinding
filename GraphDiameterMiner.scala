/*
Name: GraphDiameterMiner
Do connected component and longest path finding using pregel API
Output: VertexId, LongestPath, Lowest VertexId In the Connected Graph
Author: linhots.tsao@gmail.com
*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib
import org.apache.spark.graphx.GraphOps
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

trait Logs {
	private[this] val logger = Logger.getLogger(getClass().getName());

	 import org.apache.log4j.Level._

	def debug(message: => String) = if (logger.isEnabledFor(DEBUG)) logger.debug(message)
	def debug(message: => String, ex:Throwable) = if (logger.isEnabledFor(DEBUG)) logger.debug(message,ex)
	def debugValue[T](valueName: String, value: => T):T = {
	    val result:T = value
	    debug(valueName + " == " + result.toString)
	    result
	  }

	def info(message: => String) = if (logger.isEnabledFor(INFO)) logger.info(message)
	def info(message: => String, ex:Throwable) = if (logger.isEnabledFor(INFO)) logger.info(message,ex)

	def warn(message: => String) = if (logger.isEnabledFor(WARN)) logger.warn(message)
	def warn(message: => String, ex:Throwable) = if (logger.isEnabledFor(WARN)) logger.warn(message,ex)

	def error(ex:Throwable) = if (logger.isEnabledFor(ERROR)) logger.error(ex.toString,ex)
	def error(message: => String) = if (logger.isEnabledFor(ERROR)) logger.error(message)
	def error(message: => String, ex:Throwable) = if (logger.isEnabledFor(ERROR)) logger.error(message,ex)

	def fatal(ex:Throwable) = if (logger.isEnabledFor(FATAL)) logger.fatal(ex.toString,ex)
	def fatal(message: => String) = if (logger.isEnabledFor(FATAL)) logger.fatal(message)
	def fatal(message: => String, ex:Throwable) = if (logger.isEnabledFor(FATAL)) logger.fatal(message,ex)
}

object GraphDiameterMiner extends  Logs{

	def main(args: Array[String]) {
  
		//Reading a local file on HDFS
		val conf = new SparkConf().setAppName("GraphDiameterMiner").setMaster("local[1]")
		val sc = new SparkContext(conf)
	
		debug("debug")
		info("info")
		warn("warn")
		error("error")
		fatal("fatal")

//    val finput = "s3://test.tmwrs/spark_tmp/graph_in/PAS_GRAPH_EDGES_1473835_edgeonly.tab.gz"
//		val finput = "s3://test.tmwrs/spark_tmp/graph_in/PAS_GRAPH_EDGES_277325_edgeonly.tab"
//		val finput = "s3://test.tmwrs/spark_tmp/graph_in/graphedge.test"
//    val foutput_all = "s3://test.tmwrs/spark_tmp/graph_out_all"

      val finput     = "/data/T0_WRStools/graphedge.test"
      val foutput_all  = "/data/T0_WRStools/graphedge.test.result"

		def getComponentFast(joined:Graph[(Int,Option[VertexId]), Int], component: VertexId): Graph[(Int,VertexId),Int] = {
			// Filter using vertex predicate, and compare option to some
	                val filtered = joined.subgraph(vpred = {
				(vid, vdcc) => vdcc._2 == Some(component)
	                })
	                // Discard component IDs.
	                filtered.mapVertices {
	                  (vid, vdcc) => (vdcc._1,vdcc._2.get)
	                }
         	}

        	def genConnectedComponentFast(graph: Graph[Int, Int]): Array[Graph[(Int,VertexId), Int]] = {
	                // get connect graph: Graph[VertexId,Int]
			val ccGraph = graph.connectedComponents()

			info("graph.connectedComponents done")

			// a array of VertexId
	                val distinctComponent = ccGraph.vertices.map(data=>data._2).distinct()
	                // Join component ID to the original graph.
			// join (VertexId, Int) with (VertexId, VertexId)
	                val joined = graph.outerJoinVertices(ccGraph.vertices) {
	                  (vid, vd, cc) => (vd, cc)
	                }                				

			info("graph.outerJoinVertices")

			// after outerJoin, (vid, cc)
			// joined: Graph[(Int, Option[VertexId]),Int]
	                var subGraphList = Array[Graph[(Int,VertexId), Int]]() 
			distinctComponent.take(100).foreach(component => { 
				try{
				val subGraph = getComponentFast(joined, component); 
				subGraphList = subGraphList :+ subGraph;
				} finally
				{
					
				}
				fatal("distinctComponent.collect.foreach"+component)
			})
			// return a graph
	                println("===SubGraphList count:"+subGraphList.size+"===")
	                subGraphList
		}
		  
		val urlgraph = GraphLoader.edgeListFile(sc, finput)	

		var graphs = genConnectedComponentFast(urlgraph)

		info("genConnectedComponentFast done")

		var subGraphList = Array[Graph[(Int,VertexId), Int]]()
		
		graphs.foreach(g => {
			val gsize = g.vertices.count.toInt
			//val gsize = 3
			def sendMessage(edge: EdgeTriplet[(Int,VertexId),Int]): Iterator[(VertexId, Int)] = {
							 Iterator( (edge.dstId, edge.attr + edge.srcAttr._1 )  )
							  }
			val initialMessage = 0
			val rg = Pregel(g, initialMessage, activeDirection = EdgeDirection.Out,maxIterations = gsize)(
							  vprog = (id, attr, msg) => (math.max(attr._1, msg),attr._2),
							  sendMsg = sendMessage,
							  mergeMsg = (a, b) => math.max(a, b))
			subGraphList = subGraphList:+rg
		})		
		
		var allv  = subGraphList(0).vertices++subGraphList(0).vertices.distinct()

		info("graphs.foreach find longest path pregel done")

		// vid, pathlenth, lowest vid
		// subGraphList.filter(g => g.vertices.count > 2)
		subGraphList.foreach(g => {
			val lp = g.vertices.map(x=>x._2._1).max
			val lvid = g.vertices.take(1)(0)._2._2
			g.vertices.saveAsTextFile(foutput_all+"_subgraph_"+lvid+"-"+lp)
		})

	}
}
