package serving.utils

import com.intel.analytics.zoo.serving.utils.ClusterServingHelper
import org.apache.log4j.Logger

class SerParams(helper: ClusterServingHelper) extends Serializable {
  val redisHost = helper.redisHost
  val redisPort = helper.redisPort.toInt
  val coreNum = helper.coreNum
  val filter = helper.filter
  val C = helper.dataShape(0)
  val H = helper.dataShape(1)
  val W = helper.dataShape(2)
  val modelType = helper.modelType
  val chwFlag = false
  val model = helper.loadInferenceModel()
}
