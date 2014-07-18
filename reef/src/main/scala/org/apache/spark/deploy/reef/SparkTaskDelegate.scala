package org.apache.spark.deploy.reef

import org.apache.spark.Logging

/**
 * SparkTasKDelegate class for Spark-REEF API
 * executes CoarseGrainedExecutorBackend for
 * each REEF Task created.
 */
final class SparkTaskDelegate extends Logging {

  def run(args: Array[String]) {
    logInfo("Spark Executor Launched")
    //simply run CoarseGrainedExecutorBackend inside REEF Task
    org.apache.spark.executor.CoarseGrainedExecutorBackend.main(args)
  }

}