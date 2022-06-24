package org.apache.spark.graphx

object TypeAlias {
  //PrimitiveVector itself is package private,
  type PrimitiveVector[T] = org.apache.spark.util.collection.PrimitiveVector[T]
}
