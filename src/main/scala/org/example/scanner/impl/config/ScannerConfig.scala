package org.example.scanner.impl.config

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.util.SerializableConfiguration
import org.example.scanner.evaluation.RegexValidation
import org.example.scanner.impl.column.ColNameRLikeMode
import org.example.scanner.impl.column.ColNameRLikeMode.ColNameRLikeModeType
import org.example.scanner.impl.utils.{Checksums, FileUtils}
import org.example.scanner.sdk.ConfidenceEnum.{Confidence, HIGH_CONFIDENCE, LOW_CONFIDENCE, MEDIUM_CONFIDENCE, NO_MATCH, findValue}
import scala.collection.JavaConverters._
import org.yaml.snakeyaml.Yaml

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

case class ScannerConfig(
                          modelPath: Option[String],
                          configPath: Option[String],
                          hadoopConf: Option[SerializableConfiguration],
                          numCoresPerExecutor: Int
                        )
object ScannerConfig extends SLF4JLogging {

  private var writeMode = false
  private[config] val writableConfig: mutable.Map[String, Any] = mutable.Map.empty
  private var engineConfigMap: Map[String, Any] = Map.empty
  var modelPath: Option[String] = None
  var numCoresPerExecutor: Int = _
  var isInit: Boolean = false
  var disableCheck: Boolean = false
  private val defaultConfigName = "scanner-config.yml"
  private var configFilename = defaultConfigName
  private[config] val regexKey: String = "regex"
  private[config] val checksumEnableKey: String = "checksum_enable"
  private[config] val checksumFunctionKey: String = "checksum_function"
  private[config] val ChecksumCleanCharsKey: String = "checksum_clean_chars"
  private[config] val ChecksumFailConfidenceKey: String = "checksum_fail_confidence"

  def default(): ScannerConfig = ScannerConfig(None, None, None, 1)

  def checkInit(): Unit = if (!isInit && !disableCheck) throw new ScannerConfigNotLoadedException

  def withSparkNLP(modelPath: String): ScannerConfig = ScannerConfig(Some(modelPath), None, None, 1)

  private def getOrCreateSubmaps(key: String): mutable.Map[String, Any] = {
    var currentMap: mutable.Map[String, Any] = writableConfig
    key
      .split('.')
      .foreach {
        subkey =>
          currentMap = currentMap
            .getOrElseUpdate(subkey, mutable.Map.empty[String, Any])
            .asInstanceOf[mutable.Map[String, Any]]
      }
    currentMap
  }

  private def yamlToMap(yamlData: Any): Any =
    yamlData match {
      case map: java.util.Map[String, _] => map.asScala.map { case (key, value) => key -> yamlToMap(value) }.toMap
      case map: java.util.LinkedHashMap[String, _] =>
        map.asScala.map { case (key, value) => key -> yamlToMap(value) }.toMap
      case list: java.util.ArrayList[_] => list.asScala.map(yamlToMap).toList
      case str: java.lang.String        => str
      case int: java.lang.Integer       => int.asInstanceOf[Int]
      case dbl: java.lang.Double        => dbl.asInstanceOf[Double]
      case boo: java.lang.Boolean       => boo.asInstanceOf[Boolean]
      case chr: java.lang.Character     => chr.asInstanceOf[Char]
      case _                            => yamlToMap(_)
    }

  private def loadConfig(loadFromJar: Boolean = false): Map[String, Any] =
    Try {
      val fileContent =
        if (loadFromJar) FileUtils.getResourceFromJar(configFilename).mkString("\n")
        else FileUtils.getResource(configFilename).mkString("\n")
      val engineConfig = new Yaml().load(fileContent).asInstanceOf[java.util.Map[String, Any]]
      if (null == engineConfig) {
        log.warn(s"Config from $configFilename is empty!")
        Map.empty[String, Any]
      } else yamlToMap(engineConfig).asInstanceOf[Map[String, Any]]
    } match {
      case Failure(e) =>
        log.warn(s"Error reading or parsing the YAML file", e)
        Map.empty
      case Success(value) => value
    }

  def loadScannerConfig(config: ScannerConfig): Unit = {
    log.debug(s"Loading Scanner config (isInit:$isInit) with config: $config")
    if (!isInit) {
      modelPath = config.modelPath
      numCoresPerExecutor = config.numCoresPerExecutor
      FileUtils.rootPath = config.configPath
      config.hadoopConf.foreach(FileUtils.setFs)
      engineConfigMap = loadConfig()
      isInit = true
    }
  }
  private def getOrCreateKey(key: String, defaultValue: Any): Option[Any] = {
    val subkeys = key.split('.')
    val lastKey = subkeys.last
    val currentMap = getOrCreateSubmaps(subkeys.init.mkString("."))
    currentMap.put(lastKey, defaultValue)
  }

  @tailrec
  private def extractValue(keys: List[String], currentMap: Map[String, Any]): Option[Any] =
    keys match {
      case Nil         => None
      case head :: Nil => currentMap.get(head)
      case head :: tail => currentMap.get(head) match {
        case Some(value: Map[String, Any]) => extractValue(tail, value)
        case _                             => None
      }
    }

  /**
   * This function gets the value for the given nested key using dot-separated notation
   *
   * @param key
   *   the concatenated keys using dot-separated notation.
   * @param map
   *   the map to extract.
   * @tparam T
   *   output type to cast the value
   * @return
   *   the value of the last level key.
   */
  private[config] def getKey[T <: Any](key: String, map: Map[String, Any]): Option[T] =
    Try(extractValue(key.split('.').toList, map).map(_.asInstanceOf[T])) match {
      case Success(x) => x
      case Failure(e) => throw new ParseConfigException(key, e)

    }

  /**
   * Method to obtain any given key in Scanner configuration
   *
   * @param key
   *   the given key to look for.
   * @param defaultValue
   *   the default value to return if no value was found.
   * @tparam T
   *   Type to cast the extracted value.
   * @return
   *   The extracted value from the configuration file.
   */
  def getOrElse[T <: Any](key: String, defaultValue: T): T =
    if (writeMode) {
      getOrCreateKey(key, defaultValue)
      defaultValue
    } else getKey(key, engineConfigMap).getOrElse(defaultValue)

  /**
   * This function gets the value for the given nested key using dot-separated notation and applies the given function.
   * This function also will throw an exception if the given function throws any error
   *
   * @param key
   *   the concatenated keys using dot-separated notation.
   * @param map
   *   the map to extract.
   * @param f
   *   function to apply on the extracted value
   * @tparam T
   *   output type to cast the value of the config
   * @tparam U
   *   output type of the function to apply
   * @return
   *   the value of the last level key.
   */
  private def getKeyWithMap[T, U <: Any](key: String, map: Map[String, Any])(f: T => U): Option[U] =
    getKey[T](key, map).map(e => Try(f(e))) match {
      case Some(value) => value match {
        case Success(value)                                => Some(value)
        case Failure(e) if e.isInstanceOf[ConfigException] => throw e
        case Failure(e)                                    => throw new ParseConfigException(key, e)
      }
      case None => None
    }

  private def getKeyWithMapFail[T, U <: Any](key: String, map: Map[String, Any])(f: T => U): U =
    getKeyWithMap(key, map)(f) match {
      case Some(value) => value
      case None        => throw new ConfigNotFoundException(key)

    }

  /**
   * Function to parse the validations from config. If any key is invalid, the program will fail
   *
   * @param configConfidencesMap
   *   confidence list map extracted from config
   * @param confidence
   *   current confidence
   * @return
   *   a valid list of elements if everything is correctly configured
   */
  private[config] def parseRegexValidation(
                                            configConfidencesMap: List[Map[String, Any]],
                                            confidence: Confidence
                                          ): List[RegexValidation] =
    configConfidencesMap.map {
      configConfidenceMap =>
        val regex = getKeyWithMapFail[String, Regex](regexKey, configConfidenceMap)(_.r)
        val checksumEnable: Boolean = getKey[Boolean](checksumEnableKey, configConfidenceMap).getOrElse(false)
        if (!checksumEnable) RegexValidation(confidence = confidence, regex = regex)
        else {
          val checksumFunction =
            getKeyWithMapFail[String, Checksums.ChecksumFunction](checksumFunctionKey, configConfidenceMap)(
              Checksums.checksumsMap(_)
            )
          val checksumCleanChars =
            getKeyWithMap[List[String], List[Char]](ChecksumCleanCharsKey, configConfidenceMap)(_.map(_.head))
              .getOrElse(List.empty)
          val checksumFailConfidence =
            getKeyWithMap[String, Confidence](ChecksumFailConfidenceKey, configConfidenceMap) {
              strConfidence =>
                val failConf = findValue(strConfidence)
                if (failConf >= confidence) throw new InvalidConfigException(
                  s"Fail confidence [$ChecksumFailConfidenceKey]($failConf) must be smaller than result confidence ($confidence)."
                )
                else failConf
            }.getOrElse(NO_MATCH)
          RegexValidation(
            confidence = confidence,
            regex = regex,
            checksumFunction = checksumFunction,
            checksumCleanChars = checksumCleanChars,
            checksumFailConfidence = checksumFailConfidence
          )
        }
    }

  private def isNilFunction(key: String): Boolean =
    getKeyWithMap[Any, Any](key, engineConfigMap)(a => a) match {
      case Some(value) => value match {
        case str: String => Seq("null", "nil").contains(str)
        case _           => false
      }
      case None => false
    }

  /**
   * Tries to extract and parse the validations from the given confidence and key
   * @param key
   *   key to extract
   * @param confidence
   *   confidence to extract
   * @return
   *   the parsed confidence validations
   */
  private[config] def getRegexValidations(key: String, confidence: Confidence): Option[List[RegexValidation]] = {
    val completeKey = s"$key.${confidence.get}"

    if (isNilFunction(key)) Some(List.empty)
    else {
      getKeyWithMap[List[Map[String, Any]], List[RegexValidation]](completeKey, engineConfigMap) {
        confidenceListMap => parseRegexValidation(confidenceListMap, confidence)
      }
    }
  }

  /**
   * Method to obtain [[ColNameRLikeModeType]] from Scanner configuration
   *
   * @param key
   *   the given key to look for.
   * @param defaultValue
   *   the default value to return if no value was found.
   * @return
   *   The extracted value from the configuration file.
   */
  def getColNameModeOrElse(key: String, defaultValue: ColNameRLikeModeType): ColNameRLikeModeType =
    if (writeMode) {
      getOrCreateKey(key, defaultValue)
      defaultValue
    } else {
      getKeyWithMap[String, Option[ColNameRLikeModeType]](key, engineConfigMap)(q => ColNameRLikeMode.findValue(q))
        .flatten
        .getOrElse(defaultValue)
    }

  /**
   * Tries to extract every confidence validations from a given key
   * @param key
   *   key to extract
   * @return
   *   list of parsed confidences
   */
  private[config] def getRegexValidation(key: String): Option[List[RegexValidation]] =
    List(HIGH_CONFIDENCE, MEDIUM_CONFIDENCE, LOW_CONFIDENCE).flatMap(conf => getRegexValidations(key, conf)) match {
      case list: List[List[RegexValidation]] if list.nonEmpty => Some(list.flatten)
      case _                                                  => None
    }

  /**
   * Method to obtain [[RegexValidation]] list from Scanner configuration
   *
   * @param key
   *   the given key to look for.
   * @param defaultValue
   *   the default value to return if no value was found.
   * @return
   *   The extracted value from the configuration file.
   */
  def getRegexValidationOrElse(key: String, defaultValue: List[RegexValidation]): List[RegexValidation] =
    if (writeMode) {
      val currentMap = getOrCreateSubmaps(key)
      defaultValue
        .groupBy(_.confidence)
        .foreach {
          case (confidence, regexValidations) =>
            val regexValidationsConfidenceMap = regexValidations.map {
              regexValidation =>
                val maybeChecksumKey = Checksums
                  .checksumsMap
                  .find { case (_, v) => v == regexValidation.checksumFunction }
                  .map(_._1)
                val regexValidationMap: mutable.Map[String, Any] = mutable
                  .Map(regexKey -> regexValidation.regex.toString())
                if (regexValidation.checksumEnable) regexValidationMap.put(checksumEnableKey, true)
                if (maybeChecksumKey.nonEmpty) regexValidationMap.put(checksumFunctionKey, maybeChecksumKey.get)
                if (regexValidation.checksumCleanChars.nonEmpty) regexValidationMap
                  .put(ChecksumCleanCharsKey, regexValidation.checksumCleanChars)
                if (regexValidation.checksumFailConfidence.nonEmpty) regexValidationMap
                  .put(ChecksumFailConfidenceKey, regexValidation.checksumFailConfidence.get)
                regexValidationMap
            }
            currentMap.put(confidence.get.toString, regexValidationsConfidenceMap)
        }
      defaultValue
    } else getRegexValidation(key).getOrElse(defaultValue)

  private def getValueFromMap[K, V](map: Map[K, V], key: K, errorMessage: String): V =
    map.get(key) match {
      case Some(value) => value
      case None        => throw new NoSuchElementException(errorMessage)
    }

  private def parseExternalJars(externalJarsList: List[Map[String, Any]]): List[ExternalJarInfo] =
    externalJarsList.map(
      externalJar =>
        ExternalJarInfo(
          jarPath = getValueFromMap(externalJar, "jar", "An external JAR did not have a path defined").toString,
          classes = getValueFromMap(externalJar, "classes", "An external JAR did not have classes defined")
            .asInstanceOf[Seq[String]]
        )
    )

  /**
   * Tries to extract and parse the external jars for the custom functions
   *
   * @return
   *   the parsed external jars
   */
  def getExternalJarInfos: List[ExternalJarInfo] = {
    val customFunctionsKey = "customs"
    if (isNilFunction(customFunctionsKey)) List.empty[ExternalJarInfo]
    else getKeyWithMap[List[Map[String, Any]], List[ExternalJarInfo]](customFunctionsKey, engineConfigMap) {
      externalJarsList => parseExternalJars(externalJarsList)
    }.getOrElse(List.empty[ExternalJarInfo])
  }
}

sealed trait ConfigException

private class InvalidConfigException(msg: String) extends Exception(msg) with ConfigException

private class ParseConfigException(key: String, thr: Throwable)
  extends Exception(s"Unable to parse [$key] in Scanner config YAML", thr) with ConfigException

private class ConfigNotFoundException(key: String)
  extends Exception(s"Key [$key] in Scanner config YAML is not found and its mandatory.") with ConfigException

private class ScannerConfigNotLoadedException extends Exception(s"Fatal Error: ScannerConfig is not loaded")