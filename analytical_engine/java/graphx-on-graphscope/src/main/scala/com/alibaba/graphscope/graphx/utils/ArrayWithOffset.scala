package com.alibaba.graphscope.graphx.utils

import scala.reflect.ClassTag

class ArrayWithOffset[@specialized(Long,Int,Double,Float) T :ClassTag](val offset : Int, val array: Array[T]){

  def this(offset : Int, length : Int){
    this(offset, new Array[T](length))
  }

  @inline
  def length : Int = array.length

  @inline
  def apply(i : Int): T = array(i - offset)

  @inline
  def update(i : Int, value: T): Unit=  {
    array.update(i-offset,value)
  }

  override def clone() : ArrayWithOffset[T] = {
    new ArrayWithOffset[T](offset, length)
  }
}
