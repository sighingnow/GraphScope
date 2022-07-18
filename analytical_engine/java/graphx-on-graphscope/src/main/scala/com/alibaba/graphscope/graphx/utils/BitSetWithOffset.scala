package com.alibaba.graphscope.graphx.utils

import org.apache.spark.util.collection.BitSet

class BitSetWithOffset(val startBit : Int, val endBit : Int, val bitset : BitSet) {
  def this(startBit: Int, endBit : Int)  = {
    this(startBit,endBit,new BitSet(endBit - startBit))
  }
  require(endBit > startBit)
  val size = endBit - startBit

  def set(bit : Int) : Unit = {
    check(bit)
    bitset.set(bit - startBit)
  }

  /** [a,b) */
  def setRange(a : Int, b :Int) : Unit = {
    require(a >= startBit && a < endBit, s"${a} out of range ${startBit},${endBit}")
    require(b > a && b > startBit && b <= endBit, s"${a} out of range ${startBit},${endBit}")
    var i = a - startBit
    val limit = b - startBit
    while (i < limit){
      bitset.set(i)
      i += 1
    }
  }

  def get(bit : Int) : Boolean = {
    check(bit)
    bitset.get(bit - startBit)
  }

  def unset(bit : Int) : Unit = {
    check(bit)
    bitset.unset(bit - startBit)
  }

  @inline
  def check(bit : Int) : Unit = {
    require(bit >= startBit && bit <= endBit, s"index of range ${bit}, range [${startBit},${endBit})")
  }

  def union(other : BitSetWithOffset): Unit ={
    require(size == other.size && (startBit == other.startBit) && (endBit == other.endBit), s"can not union between ${this.toString} and ${other.toString}")
    bitset.union(other.bitset)
  }

  def &(other : BitSetWithOffset) : BitSetWithOffset = {
    require(size == other.size && (startBit == other.startBit) && (endBit == other.endBit), s"can not union between ${this.toString} and ${other.toString}")
    new BitSetWithOffset(startBit, endBit, bitset & other.bitset)
  }

  def cardinality() : Int = bitset.cardinality()

  def capacity : Int = bitset.capacity

  @inline
  def nextSetBit(bit : Int) : Int = {
//    if (bit >= endBit) return -1
//    check(bit)
    bitset.nextSetBit(bit)
  }

  def andNot(other: BitSetWithOffset) : BitSetWithOffset = {
    require(size == other.size && (startBit == other.startBit) && (endBit == other.endBit), s"can not union between ${this.toString} and ${other.toString}")
    new BitSetWithOffset(startBit,endBit, bitset.andNot(other.bitset))
  }

  override def toString: String = "BitSetWithOffset(start=" + startBit + ",end=" + endBit;
}
