package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.util.{CallSite, Utils}

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

class SparkContext extends Logging {
  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override def childValue(parent: Properties): Properties = {
      // Note: make a clone such that changes in the parent properties aren't reflected in
      // the those of the children threads, which has confusing semantics (SPARK-10563).
      Utils.cloneProperties(parent)
    }
    override protected def initialValue(): Properties = new Properties()
  }
  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).orNull

  private[spark] def getCallSite(): CallSite = {
    lazy val callSite = Utils.getCallSite()
    CallSite(
      Option(getLocalProperty(CallSite.SHORT_FORM)).getOrElse(callSite.shortForm),
      Option(getLocalProperty(CallSite.LONG_FORM)).getOrElse(callSite.longForm)
    )
  }
  private val nextRddId = new AtomicInteger(0)

  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

}
