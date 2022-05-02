package org.apache.spark.graphx.traits

import com.alibaba.graphscope.parallel.DefaultMessageManager

trait MessageStore[MSG] {
  def messageAvailable(lid: Long): Boolean

  def hasMessages: Boolean

  def getMessage(lid: Long): MSG

  def addLidMessage(lid: Long, msg: MSG): Unit

  def addOidMessage(oid: Long, msg: MSG): Unit

  def clear(): Unit

  def swap(messageStore: MessageStore[MSG]): Unit

  def flushMessage(messageManager: DefaultMessageManager): Unit
}
