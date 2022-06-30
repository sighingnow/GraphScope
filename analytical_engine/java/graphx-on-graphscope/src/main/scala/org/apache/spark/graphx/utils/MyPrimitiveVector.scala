package org.apache.spark.graphx.utils

import scala.reflect.ClassTag

class MyPrimitiveVector[T : ClassTag](initialSize : Int = 64) extends org.apache.spark.util.collection.PrimitiveVector[T](initialSize) with Serializable {

}
