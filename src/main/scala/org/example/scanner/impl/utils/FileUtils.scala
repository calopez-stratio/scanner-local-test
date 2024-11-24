package org.example.scanner.impl.utils

import akka.event.slf4j.SLF4JLogging
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.util.SerializableConfiguration

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

object FileUtils extends SLF4JLogging {

  private var hdfsClient: Option[FileSystem] = None
  private val DefaultTestFilesPath: String = "src/test/resources/"
  private var isTesting: Boolean = false

  private def getBufferFromHDFS(hdfsPath: String): Option[FSDataInputStream] =
    hdfsClient.flatMap {
      client =>
        Try {
          log.info(s"Getting buffer $hdfsPath from HDFS")
          client.open(new Path(hdfsPath))
        } match {
          case Failure(e) =>
            log.warn(s"Error getting file from HDFS", e)
            None
          case Success(buffer) =>
            log.debug(s"Successful obtained file from HDFS.")
            Some(buffer)
        }
    }

  def getIsInResource(keyword: String): HashSet[String] = {
    val resourceName = s"$keyword.txt"
    val localResource = getResourceFromJar(resourceName)
    val combinedResources = getResourceFromFS(resourceName).map(readBuffer) match {
      case Some(someLoadedList) => someLoadedList ++ localResource
      case None                 => localResource
    }
    combinedResources.map(_.toUpperCase).to[HashSet]
  }

  /**
   * Given a text file name returns a sequence with each line of it.
   *
   * @param filename
   *   Relative file path (from [[FileUtils.rootPath]]) to the text file.
   * @return
   *   Sequence of [[String]] with each line of text file.
   */
  def getFileContainsResource(filename: String): Seq[String] = {
    val combinedResources =
      // TODO: Decouple test and production code
      if (isTesting) { getResourceFromLocal(getTestFilesPath + filename) }
      else getResourceFromFS(filename).map(readBuffer) match {
        case Some(fsResource) if fsResource.nonEmpty => fsResource
        case _                                       =>
          // TODO: Add logger or exception
          Seq.empty[String]
      }
    combinedResources
  }
  private def getResourceFromFS(resourceName: String): Option[BufferedSource] =
    if (hdfsClient.isDefined && rootPath.isDefined) getBufferFromHDFS(s"${rootPath.get}/$resourceName")
      .map(Source.fromInputStream)
    else None
  var rootPath: Option[String] = None
  def setFs(sparkContext: SparkContext, fileSystemOpt: Option[FileSystem]): Unit =
    hdfsClient = Some(fileSystemOpt.getOrElse(FileSystem.get(sparkContext.hadoopConfiguration)))

  def setFs(hadoopConf: SerializableConfiguration): Unit = hdfsClient = Some(FileSystem.get(hadoopConf.value))

  def cleanString(str: String): String =
    str.replaceAllLiterally("\\\\", "\\").replaceAllLiterally("\\\"", "\"").drop(1).dropRight(1)

  private def readBuffer(buffer: BufferedSource): Seq[String] = {
    val lines = mutable.ListBuffer[String]()
    for (line <- buffer.getLines) lines += line
    buffer.close
    lines
  }
  def getResource(resourceName: String): Seq[String] = {
    log.info(s"Extracting $resourceName using ${if (hdfsClient.isDefined) "hadoop" else "local resource file"}")
    getResourceFromFS(resourceName).map(readBuffer).getOrElse(Seq.empty)
  }

  def copyHdfsFileToLocal(hdfsPath: Path, localPath: String): Unit = {
    val hdfsInputStream = hdfsClient.get.open(hdfsPath)
    val localOutputStream = new FileOutputStream(new File(localPath))

    val buffer = new Array[Byte](1024)
    var bytesRead = hdfsInputStream.read(buffer)
    while (bytesRead > 0) {
      localOutputStream.write(buffer, 0, bytesRead)
      bytesRead = hdfsInputStream.read(buffer)
    }

    localOutputStream.close()
    hdfsInputStream.close()
  }

  def getResourceFromJar(resourceName: String): Seq[String] = {
    log.info(s"Reading from local JAR $resourceName")
    Try {
      val in = getClass.getResourceAsStream(s"/$resourceName")
      val reader = new BufferedReader(new InputStreamReader(in))
      val lines = mutable.ListBuffer[String]()
      reader.lines().forEach(line => lines += line)
      reader.close()
      in.close()
      lines
    } match {
      case Failure(exception) =>
        log.warn(s"Unable to find $resourceName on local JAR. Cause: ${exception.getCause}")
        Seq.empty
      case Success(value) => value
    }
  }

  def downloadJarToTmp(hdfsPath: String): Option[String] = {
    log.info(s"Downloading JAR from HDFS $hdfsPath to local tmp path.")
    val internalPath =
      if (hdfsPath.startsWith("hdfs://")) { hdfsPath.substring("hdfs://".length) }
      else { hdfsPath }
    getBufferFromHDFS(internalPath) match {
      case Some(hdfsBuffer) =>
        val tempJar = File.createTempFile("custom-function", ".jar")
        log.debug(s"Saving JAR to temp file $tempJar")
        Try {
          val outputStream = new FileOutputStream(tempJar)
          val buffer = new Array[Byte](1024)
          var bytesRead = hdfsBuffer.read(buffer)
          while (bytesRead > 0) {
            outputStream.write(buffer, 0, bytesRead)
            bytesRead = hdfsBuffer.read(buffer)
          }
          outputStream.close()
          hdfsBuffer.close()
        } match {
          case Failure(exception) =>
            log.error("Unable to save to local JAR", exception)
            None
          case Success(_) =>
            log.info(s"Successfully saved JAR to temp file")
            Some(tempJar.getPath)
        }
      case None =>
        log.warn(s"Unable to find JAR $hdfsPath in HDFS")
        None
    }
  }

  private def getTestFilesPath: String = {
    val rootPath: String = getClass.getResource(".").toString.split("target").head.replace("file:", "")
    rootPath + DefaultTestFilesPath
  }

  def getResourceFromLocal(path: String): Seq[String] = {
    log.info(s"Reading from local path $path")
    val file = new File(path)
    val fileReader = new FileReader(file)
    val reader = new BufferedReader(fileReader)
    val lines = mutable.ListBuffer[String]()
    reader.lines().forEach(line => lines += line)
    reader.close()
    fileReader.close()
    lines
  }

  def getInFileResource(filename: String): HashSet[String] = {
    val combinedResources =
      if (isTesting) { getResourceFromLocal(getTestFilesPath + filename) }
      else getResourceFromFS(filename).map(readBuffer).getOrElse(Seq.empty[String])
    combinedResources.map(_.toUpperCase).to[HashSet]
  }
}
