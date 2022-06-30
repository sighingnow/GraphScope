package org.apache.spark.graphx.utils

class MyPrimitiveVector[T](initialSize : Int = 64) extends org.apache.spark.util.collection.PrimitiveVector[T](initialSize) with Serializable {

}
