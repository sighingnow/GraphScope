package org.apache.spark.graphx

import com.alibaba.graphscope.graphx.GraphScopeHelper
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

object GraphLoader extends Logging{
  def edgeListFile(
                    sc: SparkContext,
                    path: String,
                    canonicalOrientation: Boolean = false,
                    numEdgePartitions: Int = -1,
                    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : Graph[Int, Int] = {
    GraphScopeHelper.edgeListFile(sc, path, canonicalOrientation, numEdgePartitions, edgeStorageLevel, vertexStorageLevel)
  }
}
