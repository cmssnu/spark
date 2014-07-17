package org.apache.spark.deploy.reef

import java.io.{File, FileInputStream, IOException}
import java.net.URL
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.executor.ExecutorURLClassLoader
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkException}

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
  var sparkConf: SparkConf = null
  // variable used to notify the REEFClusterScheduler
  val doneWithREEFEvaluatorInitMonitor = new Object() // lock for REEF Evaluator
  val doneWithSparkContextInitMonitor = new Object() //lock for SparkContext
  @volatile var isDoneWithSparkContextInit = false
  @volatile var isDoneWithREEFEvaluatorInit = false
  var isFinished: Boolean = false

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
   * Called from REEFDriver.
   * Runs userClass in a separate Thread.
   *
   * @param userClass user class
   * @param userArgs user arguments
   * @return Thread
   */
  def startUserClass(userClass: String, userArgs: Array[String]): Thread = {
    logInfo("Starting the user class in a separate Thread")

    val loader = new ExecutorURLClassLoader(new Array[URL](0),
      Thread.currentThread.getContextClassLoader)
    Thread.currentThread.setContextClassLoader(loader)


    //retrieve log4j.prop here
    val url = loader.getResource("log4j-spark-reef.properties")
    PropertyConfigurator.configure(url)

    // Add spark.jars here.
    // REEFDriver currently does not add jars to user class path.
    val jars = System.getProperty("spark.jars").split(",")
    for (jar <- jars) {
      addJarToClasspath(jar, loader)
    }

    val mainClass = Class.forName(userClass, true, loader)
    val mainMethod = mainClass.getMethod("main", classOf[Array[String]])

    val t = new Thread {
      override def run() {
        var succeeded = false
        try {
          mainMethod.invoke(null, userArgs)
          succeeded = true
        } catch{
          case _: Throwable =>
            logError("Could not finish User Class")
            throw new RuntimeException("Error running user class")
        }
      }
    }
    t.setName("User Class Main")
    t.start()
    t
  }

  /**
   *
   * Called by startUserClass().
   * Adds user jars to user class path.
   *
   * @param localJar user jars
   * @param loader ExecutorURLClassLoader
   */
  private def addJarToClasspath(localJar: String, loader: ExecutorURLClassLoader) {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          logWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        logWarning(s"Skip remote jar $uri.")
    }
  }

  /**
   *
   * Called from REEFClusterScheduler.
   * Notify that a SparkContext has been initialized in the user class.
   *
   * @param sc SparkContext
   * @return Boolean
   */
  def sparkContextInitialized(sc: SparkContext): Boolean = {
    var modified = false
    sparkContextRef.synchronized {
      modified = sparkContextRef.compareAndSet(null, sc)
      //sparkContextRef.notifyAll()
      // Add a shutdown hook in case users do not call sc.stop or do System.exit.
      if (modified) {
        val sparkContext = sparkContextRef.get()
        val sparkConf = sparkContext.getConf
        //retrieve spark driver url for SparkExecutorTask
        REEFDriverDelegate.driverURL = "akka.tcp://spark@%s:%s/user/%s".format(
          sparkConf.get("spark.driver.host"),
          sparkConf.get("spark.driver.port"),
          CoarseGrainedSchedulerBackend.ACTOR_NAME)
        Runtime.getRuntime().addShutdownHook(new Thread with Logging {
          logInfo("Adding shutdown hook for SparkContext " + sc)

          override def run() {
            logInfo("Invoking sc stop from shutdown hook")
            sc.stop()
          }
        })
      } else{
        logWarning("Unable to retrieve SparkContext")
        throw new RuntimeException("SparkContext not initialized")
      }
      modified
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
   * REEFClusterScheduler has to acquire lock
   * before it proceeds with user class.
   */
  def waitForREEFEvaluatorInit() {
    doneWithREEFEvaluatorInitMonitor.synchronized {
      while (!isDoneWithREEFEvaluatorInit) {
        doneWithREEFEvaluatorInitMonitor.wait(1000L)
      }
    }
  }

}