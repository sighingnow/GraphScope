package org.apache.spark.graphx.test

// $example on$
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * A connected components algorithm example.
 * The connected components algorithm labels each connected component of the graph
 * with the ID of its lowest-numbered vertex.
 * For example, in a social network, connected components can approximate clusters.
 * GraphX contains an implementation of the algorithm in the
 * [`ConnectedComponents` object][ConnectedComponents],
 * and we compute the connected components of the example social network dataset.
 *
 * Run with
 * {{{
 * bin/run-example graphx.ConnectedComponentsExample
 * }}}
 */
object CCTest extends Logging{
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 3) {
      println("Expect 1 args")
      return 0;
    }
    val efile = args(0)
    val partNum = args(1).toInt
    val sourceId = args(2).toLong
    val graph: Graph[Int, Int] =
      GraphLoader.edgeListFile(sc, efile,canonicalOrientation = false, partNum)

    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Join the connected components with the usernames

    cc.saveAsTextFile(s"/tmp/cc-test-${java.time.LocalDateTime.now()}")
    spark.stop()
  }
}

