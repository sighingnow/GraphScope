package org.apache.spark.graphx.utils

class MyPrimitiveVector[T](initialSize : Int) extends org.apache.spark.util.collection.PrimitiveVector[T](initialSize) with Serializable {

}
