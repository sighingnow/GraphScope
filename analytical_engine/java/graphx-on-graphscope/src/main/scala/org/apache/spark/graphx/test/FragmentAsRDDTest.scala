package org.apache.spark.graphx.test

import org.apache.spark.graphx.impl.GrapeGraphImpl
import org.apache.spark.graphx.rdd.GraphScopeRDD
import org.apache.spark.graphx.rdd.GraphScopeRDD.{log, makeRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.net.InetAddress
import scala.collection.mutable

object FragmentAsRDDTest extends Logging{
  def main(array: Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
//    sc.setLogLevel("debug")
    require(array.length == 2)
    val objectIDs = array(0)
    val fragName = array(1)

    val objectsSplited: Array[String] = objectIDs.split(",")
    val map: mutable.Map[String,Long] = mutable.Map[String, Long]()
    for (str <- objectsSplited){
      val hostId = str.split(":")
      require(hostId.length == 2)
      val host = InetAddress.getByName(hostId(0)).getHostAddress
      val id = hostId(1)
      require(!map.contains(host), s"entry for host ${host} already set ${map.get(host)}")
      map(host) = id.toLong
      log.info(s"host ${host}: objectId : ${id}")
    }
    val res = map.zipWithIndex.map(tuple => (tuple._1._2, Array(TaskLocation.executorLocationTag + tuple._1._1 + "_" + tuple._2))).toArray
    val distributedObjectIDs = makeRDD(sc,res).persist(StorageLevel.MEMORY_ONLY)
    distributedObjectIDs.foreachPartition(iter => log.info(s"one partition on ${InetAddress.getLocalHost.getHostName}"))
    log.info(s"${distributedObjectIDs.collect().mkString("Array(", ", ", ")")}")

    log.info(s"Getting fragment ${objectIDs} as RDD, frag type ${fragName}")
    val (vertexRDD,edgeRDD) = GraphScopeRDD.loadFragmentAsRDD[Double,Long](sc, objectIDs, fragName)
    log.info(s"vertices count ${vertexRDD.count()}, edge cout ${edgeRDD.count()}")

    //1. map vertices
    val v1 = vertexRDD.mapVertices((vid, vd)=> {
      log.info(s"visiting vertex (${vid},${vd})")
      vid + 1
    })
//    val v2 = v1.mapVertices((vid, vd)=> vid + 2)
//    val v3 = v2.mapVertices((vid, vd)=> vid + 3)
    log.info(s"mapping vertices num ${v1.count()}")

    //2. map edges
    val e1 = edgeRDD.mapValues(edge => {
      log.info(s"visiting edge (${edge.srcId}, ${edge.dstId}, ${edge.attr})")
      edge.attr + 1
    })
//    val e2 = e1.mapValues(edge => edge.attr + edge.dstId)
//    val e3 = e2.mapValues(edge => edge.attr + edge.srcId)
    log.info(s"mapping edges num ${e1.count()}")

    //3. map triplet
    val graph = GrapeGraphImpl.fromExistingRDDs(vertexRDD,edgeRDD)
    val graph2 = graph.mapTriplets(triplet => {
      log.info(s"visiting edge triplet [src: (${triplet.srcId}, ${triplet.srcAttr}), dst: (${triplet.dstId}, ${triplet.dstAttr}), attr: ${triplet.attr}]")
      triplet.srcAttr + triplet.dstAttr + triplet.attr
    })

//    val graph3 = graph.mapTriplets(triplet => triplet.attr + 1)
    log.info(s"mapping triplet,edges num ${graph2.numEdges}, vertices num ${graph2.numVertices}")
    sc.stop()
  }
}
