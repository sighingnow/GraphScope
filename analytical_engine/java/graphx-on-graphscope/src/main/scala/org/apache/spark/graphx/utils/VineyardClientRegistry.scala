package org.apache.spark.graphx.utils

import com.alibaba.fastffi.{FFIByteString, FFITypeFactory}
import com.alibaba.graphscope.graphx.VineyardClient
import org.apache.spark.internal.Logging

object VineyardClientRegistry  extends Logging{
  System.loadLibrary("grape-jni")
  val clientFactory : VineyardClient.Factory = FFITypeFactory.getFactory(classOf[VineyardClient],"vineyard::Client").asInstanceOf[VineyardClient.Factory]
  var client: VineyardClient = null.asInstanceOf[VineyardClient]

  /**
   * should only be created once.
   */
  def connect(endPoint: String) : VineyardClient = {
    if (client == null)
      synchronized{
        if (client == null){
          client = clientFactory.create()
          log.info(s"[VineyardClientRegistry]: Connecting to ${endPoint}")
          val str = FFITypeFactory.newByteString();
          str.copyFrom(endPoint)
          client.connect(str);
          log.info(s"Successfully connected to ${endPoint}")
        }
    }
    client
  }

  def getClient() : VineyardClient ={
    if (client == null){
      throw new IllegalStateException("call connect first");
    }
    client
  }
}
