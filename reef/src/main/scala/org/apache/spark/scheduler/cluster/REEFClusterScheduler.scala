package org.apache.spark.scheduler.cluster

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.deploy.reef.REEFDriverDelegate

/**
 * REEFClusterScheduler class for Spark-REEF API
 * notify REEFDriverDelegate when SparkContext is created,
 * and halts user class until REEF Evaluators are up.
 */
private[spark] class REEFClusterScheduler(sc: SparkContext) extends TaskSchedulerImpl(sc) {

  logInfo("Created REEFClusterScheduler")

  override def postStartHook() {
    val sparkContextInitialized = REEFDriverDelegate.sparkContextInitialized(sc)
    if (sparkContextInitialized){
      logInfo("Waiting for REEF Evaluators")
      REEFDriverDelegate.waitForREEFEvaluatorInit() //wait for REEF Evaluator to init
    }
    logInfo("REEFClusterScheduler.postStartHook done")
  }
}
