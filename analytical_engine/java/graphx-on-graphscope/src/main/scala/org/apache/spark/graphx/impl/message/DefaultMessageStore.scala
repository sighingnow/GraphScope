package org.apache.spark.graphx.impl.message

import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.parallel.DefaultMessageManager
import org.apache.spark.graphx.traits.{MessageStore, VertexDataManager, GraphXVertexIdManager}

import scala.reflect.ClassTag

class DefaultMessageStore[MSG: ClassTag] extends MessageStore[MSG]{
  override def init[VD](fragment: IFragment[Long, Long, _, _], idManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD], mergeMessage: (MSG, MSG) => MSG): Unit = ???

  override def messageAvailable(lid: Long): Boolean = ???

  override def hasMessages: Boolean = ???

  override def getMessage(lid: Long): MSG = ???

  override def addLidMessage(lid: Long, msg: MSG): Unit = ???

  override def addOidMessage(oid: Long, msg: MSG): Unit = ???

  override def clear(): Unit = ???

  override def swap(messageStore: MessageStore[MSG]): Unit = ???

  override def flushMessage(messageManager: DefaultMessageManager): Unit = ???
}
