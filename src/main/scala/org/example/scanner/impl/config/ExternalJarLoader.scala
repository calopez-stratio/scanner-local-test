package org.example.scanner.impl.config

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.sql.SparkSession
import org.example.scanner.impl.utils.FileUtils
import java.util
import java.nio.file.{Files, Paths}
import java.net.{URL, URLClassLoader}

import scala.collection.JavaConverters._

object ExternalJarLoader extends SLF4JLogging {

  // Variables for testing and scanner-api
  private var fromLocal = false
  private var forcedLocalPaths = Seq.empty[String]
  private def isLocal: Boolean = fromLocal && forcedLocalPaths.nonEmpty

  private var loadedSparkJars: Seq[String] = Seq.empty

  // List of LocalJarInfo loaded from scanner config
  private lazy val localJarInfo: Seq[LocalJarInfo] = {
    log.info("Preparing LocalJarInfo")
    val externalJarInfos = ScannerConfig.getExternalJarInfos
    log.info(s"There are ${externalJarInfos.size} JARs to load")
    val info =
      if (isLocal) {
        externalJarInfos
          .zip(forcedLocalPaths)
          .map {
            case (externalJar, forcedLocalPath) => LocalJarInfo(translatePath(forcedLocalPath), externalJar.classes)
          }
      } else externalJarInfos.map(externalToLocalJarInfo)
    log.info(s"LocalJarInfo is loaded: $info")
    info
  }

  // List of class names loaded from scanner config
  lazy val customClassNames: Seq[String] = {
    ScannerConfig.checkInit()
    log.info("Getting custom class names from scanner-engine config")
    val classes = localJarInfo.flatMap(_.classes)
    log.info(s"Custom class names are: ${classes.mkString(",")}")
    classes
  }

  lazy val customClassLoader: URLClassLoader = {
    log.info("Preparing custom class loader")
    val jarURLs = localJarInfo.map(_.jarPath).distinct.map(new URL(_))
    log.info(s"Custom class loader contains URLs: ${jarURLs.mkString(",")}")
    new URLClassLoader(jarURLs.toArray, getClass.getClassLoader)
  }

  // Adds custom JARs to Spark context classpath
  def addJarsToSparkContext()(implicit spark: SparkSession): Unit = {
    val jarsToAdd = localJarInfo.map(_.jarPath).distinct
    log.info(s"Adding ${jarsToAdd.size} JARs to Spark Context")
    jarsToAdd
      .filterNot(loadedSparkJars.contains)
      .foreach {
        newJar =>
          spark.sparkContext.addJar(newJar)
          loadedSparkJars = loadedSparkJars :+ newJar
      }
  }

  // Using ExternalJarInfo ensures the file is available in local filesystem
  private def externalToLocalJarInfo(externalJarInfo: ExternalJarInfo): LocalJarInfo = {
    log.info(s"Preparing external JAR $externalJarInfo")
    val jarPath = translatePath(externalJarInfo.jarPath)
    log.info(s"JAR $jarPath is in HDFS. Downloading JAR...")
    val tmpJar = FileUtils.downloadJarToTmp(jarPath)
    assert(
      tmpJar.isDefined && Files.exists(Paths.get(tmpJar.get)),
      s"Downloaded JAR from HDFS ($jarPath) is not found in local filesystem ($tmpJar)"
    )
    log.info(s"JAR tmp path is $tmpJar")
    val newPath = tmpJar.get
    LocalJarInfo(s"file://$newPath", externalJarInfo.classes)
  }

  private def translatePath(originalPath: String): String =
    if (fromLocal) s"file://$originalPath" else s"hdfs://$originalPath"

  // Methods for scanner-engine testing
  /** Sets the JARs to be loaded from local filesystem to be used in tests */
  def forceLocalPaths(paths: Seq[String]): Unit = {
    fromLocal = true
    forcedLocalPaths = paths
  }

  // Methods for scanner-api
  /**
   * Sets the local paths where JARs from scanner-engine config are located. Used in scanner-api to load JARS.
   *
   * @param paths
   *   a List of local paths where JARs are located
   */
  def forceLocalPathsAPI(paths: util.List[String]): Unit = forceLocalPaths(paths.asScala)

  /**
   * Extract from ScannerConfig the HDFS path for external jars. Used in scanner-api to load JARs.
   *
   * @return
   *   a List of paths to HDFS where external jars are located
   */
  def getHDFSPathsAPI: util.List[String] = ScannerConfig.getExternalJarInfos.map(_.jarPath).distinct.asJava
}

case class ExternalJarInfo(jarPath: String, classes: Seq[String])
case class LocalJarInfo(jarPath: String, classes: Seq[String])
