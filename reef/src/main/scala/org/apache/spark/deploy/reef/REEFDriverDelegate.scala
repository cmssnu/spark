/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.reef

import java.io.{File, FileInputStream, IOException}
import java.net.URL
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.executor.ExecutorURLClassLoader
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkContext, SparkException}

import scala.beans.BeanProperty

/**
 * REEFDriverDelegate object for Spark-REEF API
 * starts user defined class, and halts
 * Spark task scheduler until SparkContext is initialized.
 */
object REEFDriverDelegate extends Logging {

  val sparkContextRef: AtomicReference[SparkContext] = new AtomicReference[SparkContext](null)
  //create getter & setter for Spark Driver URL
  @BeanProperty var driverURL: String = null
  // variable used to notify the REEFClusterScheduler
  val doneWithREEFEvaluatorInitMonitor = new Object() // lock for REEF Evaluator
  val doneWithSparkContextInitMonitor = new Object() //lock for SparkContext
  @volatile var isDoneWithSparkContextInit = false
  @volatile var isDoneWithREEFEvaluatorInit = false

  /**
   *
   * Load properties present in the given file.
   *
   * @return
   */
  def setSystemPropertiesFromFile(file: File) {
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")
    val inputStream = new FileInputStream(file)
    try {
      val properties = new Properties()
      properties.load(inputStream)
      setSystemProperties(properties)
    } catch {

      case e: IOException =>
        val message = s"Failed when loading Spark properties file $file"
        throw new SparkException(message, e)
    } finally {
      inputStream.close()
    }
  }

  /**
   *
   * Sets up System property for SparkConf.
   *
   * @return
   */
  def setSystemProperties(sparkArgs: Properties) {
    val sparkProperty: Properties = System.getProperties
    sparkProperty.putAll(sparkArgs)
    System.setProperties(sparkProperty)
  }

  /**
   *
   * Called from SparJobHandler.
   * Runs userClass in current Thread.
   *
   * @param userClass user class
   * @param userArgs user arguments
   * @return
   */
  @throws[RuntimeException]
  def startUserClass(userClass: String, userArgs: Array[String], userJar: Array[String]) {

    val loader = new ExecutorURLClassLoader(new Array[URL](0),
      Thread.currentThread.getContextClassLoader)
    Thread.currentThread.setContextClassLoader(loader)

    // Add spark.jars here.
    for (jar <- userJar) {
      addJarToClasspath(jar, loader)
    }

    try {
      val mainClass = Class.forName(userClass, true, loader)
      val mainMethod = mainClass.getMethod("main", classOf[Array[String]])
      mainMethod.invoke(null, userArgs)
    } catch {
      case e: Throwable =>
        logError("Could not finish user class")
        throw new RuntimeException(e)
    }
  }

  /**
   *
   * Called by startUserClass().
   * Adds user jars to user class path.
   *
   * In cluster mode, Spark should assume that
   * all required jars are sent to local dir by REEF.
   *
   * @param localJar user jars
   * @param loader ExecutorURLClassLoader
   */
  private def addJarToClasspath(localJar: String, loader: ExecutorURLClassLoader) {
    val uri = Utils.resolveURI(localJar)
    logInfo(s"Received URI: $uri")
    uri.getScheme match {
      case "file" | "local" | null =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          logWarning(s"Local jar $file does not exist.")
        }
    }
  }

  /**
   *
   * Called from REEFClusterScheduler.
   * Acquire SparkContext that has been initialized in the user class.
   * Release lock to REEFContextStartHandler to notify that Spark Context is alive.
   *
   * @param sc SparkContext
   * @return Boolean or RuntimeException
   */
  @throws[RuntimeException]
  def sparkContextInitialized(sc: SparkContext): Boolean = {
    var modified = false
    sparkContextRef.synchronized {
      modified = sparkContextRef.compareAndSet(null, sc)
      // Add a shutdown hook in case users do not call sc.stop or do System.exit.
      if (modified) {
        val sparkConf = sparkContextRef.get().getConf
        //retrieve spark driver url for SparkExecutorTask
        REEFDriverDelegate.driverURL = "akka.tcp://spark@%s:%s/user/%s".format(
          sparkConf.get("spark.driver.host"),
          sparkConf.get("spark.driver.port"),
          CoarseGrainedSchedulerBackend.ACTOR_NAME)
      } else{
        logError("Unable to retrieve SparkContext")
        throw new RuntimeException()
      }
    }
    REEFDriverDelegate.doneWithSparkContextInit()
    modified
  }

  /**
   * Called from SparkJobRunner.
   * Stops SparkContext in case user class does not stops it.
   */
  def stopSparkContext() {
    sparkContextRef.get().stop()
  }

  /**
   * REEFDriverDelegate release lock to
   * notify that SparkContext is alive.
   */
  def doneWithSparkContextInit() {
    isDoneWithSparkContextInit = true
    doneWithSparkContextInitMonitor.synchronized {
      // to wake threads off wait ...
      doneWithSparkContextInitMonitor.notifyAll()
    }
  }

  /**
   * REEFContextStartHandler has to acquire
   * lock before it can acquire driverURL.
   */
  def waitForSparkContextInit() {
    doneWithSparkContextInitMonitor.synchronized {
      while (!isDoneWithSparkContextInit) {
        doneWithSparkContextInitMonitor.wait(1000L)
      }
    }
  }

  /**
   * REEFDriver release lock to
   * notify that Evaluators are up.
   */
  def doneWithREEFEvaluatorInit() {
    isDoneWithREEFEvaluatorInit = true
    doneWithREEFEvaluatorInitMonitor.synchronized {
      // to wake threads off wait ...
      doneWithREEFEvaluatorInitMonitor.notifyAll()
    }
  }

  /**
   * REEFClusterScheduler has to acquire
   * lock before it proceeds with user class.
   */
  def waitForREEFEvaluatorInit() {
    doneWithREEFEvaluatorInitMonitor.synchronized {
      while (!isDoneWithREEFEvaluatorInit) {
        doneWithREEFEvaluatorInitMonitor.wait(1000L)
      }
    }
  }

}