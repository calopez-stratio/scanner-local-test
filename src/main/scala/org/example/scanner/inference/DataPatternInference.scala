package org.example.scanner.inference

import org.apache.spark.sql.DataFrame
import akka.event.slf4j.SLF4JLogging
import org.example.scanner.dsl.ast.Comparator
import org.example.scanner.evaluation.EvaluatorMetrics
import org.example.scanner.impl.column.ColNameRLike
import org.example.scanner.impl.config.ExternalJarLoader
import org.example.scanner.impl.dslfunctions
import org.example.scanner.impl.evaluation.CountryCode
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.utils.{InferenceUtils, RegexUtils}
import org.example.scanner.pattern.{DataPattern, DataPatternComplexity, QueryWithComplexity}
import org.example.scanner.sdk.traits.impl.FunctionDSL

import scala.reflect.runtime.universe._
import scala.util.Try
object DataPatternInference extends SLF4JLogging {

  val MaxIsInSize: Int = 10
  val MaxIsInString: Int = 50

  private lazy val mirror: Mirror = runtimeMirror(getClass.getClassLoader)
  private lazy val dslfunctionsMirror: InstanceMirror = mirror.reflect(dslfunctions)
  private lazy val dslfunctionsMethods = typeOf[dslfunctions.type].decls.toList.filter(_.isMethod)

  // Method to test all queries in scanner-api
  // noinspection ScalaUnusedSymbol
  def getAllSingleQueries: List[String] = getAllDSLQueries.map(_.query).filter(q => DataPattern("t", q).testEvaluator)

  private def getAllDSLQueries: List[QueryWithComplexity] =
    dslfunctionsMethods.flatMap {
      method =>
        if (method.info.resultType.toString.contains(typeOf[FunctionDSL].toString)) {
          val methodMirror = dslfunctionsMirror.reflectMethod(method.asMethod)
          val inputParams: Option[List[Symbol]] = method.info.paramLists.headOption
          inputParams match {
            case None =>
              // No params functions, like email(), date()...
              val dsl: FunctionDSL = methodMirror().asInstanceOf[FunctionDSL]
              if (dsl.contains(ColNameRLike.keyword))
                List(QueryWithComplexity(dsl, DataPatternComplexity.NoParamFunctionWithColname))
              else List(QueryWithComplexity(dsl, DataPatternComplexity.NoParamFunction))
            case Some(List(oneParam)) =>
              // One params function
              oneParam.typeSignature match {

                // Country code functions, like is_account_number("CAN"), is_passport_number("ESP")
                case x if x =:= typeOf[CountryCodeType] =>
                  CountryCode
                    .values
                    .map {
                      code =>
                        QueryWithComplexity(
                          methodMirror(code).asInstanceOf[FunctionDSL],
                          DataPatternComplexity.CountryCodeFunctionWithParam
                        )
                    }

                // Dictionary functions with threshold: country(0.8)
                case x if x =:= typeOf[Double] => None

                // Disabled functions:
                // case x if x =:= typeOf[String] => // colNameRLike, rLike, length, literal, lower, upper
                // case x if x =:= typeOf[Set[String]] => // Function: in
                case _ =>
                  log.info(
                    s"Could not infer function ${method.name.toString}(${method.info.paramLists.headOption.map(_.toString).mkString})."
                  )
                  None
              }
            case _ =>
              log.info(
                s"Could not infer function ${method.name.toString}(${method.info.paramLists.headOption.map(_.toString).mkString})."
              )
              None
          }
        } else None
    }

  private def getAllCustomDSLQueries: Seq[QueryWithComplexity] =
    ExternalJarLoader
      .customClassNames
      .flatMap {
        className =>
          Try {
            log.info(s"Trying to instantiate ${className}")
            // Create a URLClassLoader with the URL of your JAR
            val classLoader = ExternalJarLoader.customClassLoader

            // Load the class
            val clazz: Class[_] = classLoader.loadClass(className)

            // Obtain the mirror for the current class loader
            val mirror = runtimeMirror(classLoader)

            val classSymbol = mirror.classSymbol(clazz)
            // Access the primary constructor
            val primaryConstructor = classSymbol.primaryConstructor.asMethod
            // Get the list of parameters
            val params = primaryConstructor.paramLists.flatten

            if (params.isEmpty) {
              // Get the companion object
              val companionSymbol = mirror.classSymbol(clazz).companion.asModule
              val companionObject = mirror.reflectModule(companionSymbol).instance
              // Access and invoke a function 'noParamDSL' from the companion object
              val myFunctionMethod = companionObject.getClass.getMethod("noParamDSL")
              val functionDSL = myFunctionMethod.invoke(companionObject).asInstanceOf[FunctionDSL]
              Some(QueryWithComplexity(functionDSL, DataPatternComplexity.NoParamFunction))
            } else None
          } match {
            case util.Failure(exception) =>
              log.warn(s"Error trying to instantiate $className", exception)
              None
            case util.Success(value) =>
              log.info(s"Successfully created class $className")
              value
          }
      }

  private def translateQueriesToDataPatterns(queryList: Seq[QueryWithComplexity]): Seq[DataPattern] =
    queryList
      .zipWithIndex
      .flatMap {
        case (QueryWithComplexity(query, complexity), idx) =>
          val dp = DataPattern(idx, s"Infer-Function-$idx", query, complexity)
          if (dp.testEvaluator) Some(dp)
          else {
            log.info(s"Inferred Query '$query' is not valid")
            None
          }
      }

  def combinedQueriesToDataPatterns(queryList: Seq[QueryWithComplexity]): Seq[DataPattern] =
    queryList
      .zipWithIndex
      .flatMap {
        case (QueryWithComplexity(query, complexity), idx) =>
          val dp = DataPattern(idx, s"Combined-Infer-Function-$idx", query, complexity)
          if (dp.testEvaluator) Some(dp)
          else {
            log.info(s"Combined Inferred Query '$query' is not valid")
            None
          }
      }

  // Private method to infer all kind of queries with their respective complexity
  private def inferQueriesWithComplexity(df: DataFrame): Seq[QueryWithComplexity] = {
    // Ensure dataframe is cached before applying multiple operations
    if (!df.storageLevel.useMemory) df.persist()
    // Infer all kind of queries
    getAllDSLQueries ++ getAllCustomDSLQueries ++ IsInInference.inferIsInQuery(df) ++
      RegexInference.inferRLikeQuery(df) ++ NumberInference.inferDistributionQuery(df)
  }

  /**
   * Infers data patterns from a given DataFrame by first inferring queries with complexity.
   *
   * @param df
   *   The input DataFrame to infer data patterns from.
   * @return
   *   A sequence of inferred DataPatterns.
   */
  def inferAllDataPatterns(df: DataFrame): Seq[DataPattern] =
    translateQueriesToDataPatterns(inferQueriesWithComplexity(df))

  /**
   * Infers data patterns for specified target columns of a DataFrame.
   *
   * @param df
   *   the DataFrame to analyze.
   * @param targetColumns
   *   the names of the columns to infer data patterns for.
   * @return
   *   a sequence of inferred DataPatterns.
   */
  def inferDataPatterns(dataFrames: Seq[DataFrame], targetColumns: Seq[String]): Seq[DataPattern] =
    dataFrames
      .filter(_.columns.exists(targetColumns.contains))
      .flatMap(
        df =>
          translateQueriesToDataPatterns(
            inferQueriesWithComplexity(df.select(targetColumns.head, targetColumns.tail: _*))
          )
      )

  private[inference] def inferOnlyDSLDataPatterns: Seq[DataPattern] = translateQueriesToDataPatterns(getAllDSLQueries)

  def reduceMultipleDataPatterns(dataPatterns: List[DataPattern]): String = {
    val reducedIsInPatterns = reduceIsInFunction(dataPatterns)
    val reducedRegexPatterns = reduceRegexFunction(reducedIsInPatterns)
    val (reducedDatumAndColumnPatterns, combinedColNameRLikeRegex) = reduceDatumAndColumnFunction(reducedRegexPatterns)
    val reducedRepeatedPatterns = reduceRepeatedPatterns(reducedDatumAndColumnPatterns)
    val reducedQuery = dslfunctions.buildOrQuery(reducedRepeatedPatterns.map(_.query): _*)
    if (combinedColNameRLikeRegex.nonEmpty) {
      dslfunctions.datumAndColumnFunction(reducedQuery, dslfunctions.colNameRLike(combinedColNameRLikeRegex))
    } else { reducedQuery }
  }

  /**
   * Combines multiple columns inference based on the specified target columns and corresponding metrics.
   *
   * @param targetColumns
   *   list of target columns to combine inference
   * @param metrics
   *   list of evaluator metrics
   * @return
   *   a sequence of strings representing the built queries for the combinations of target columns
   */
  def generateQueryCombinations(
                                 metrics: Seq[EvaluatorMetrics],
                                 targetColumns: Seq[String]
                               ): Seq[QueryWithComplexity] = {
    // filter and group the metrics by column name
    val columnsExtractedDSL = metrics
      .filter(c => targetColumns.contains(c.columnName))
      .groupBy(_.columnName)
      .mapValues(metrics => metrics.map(_.dataPattern.query).toSet)
      .values
      .toList
    val functionDslWithComplexity = metrics.map(metrics => metrics.dataPattern.query -> metrics.dataPattern).toMap
    val combinations = getAllSetCombinations(columnsExtractedDSL).filter(_.size > 1)
    val queriesWithComplexity = combinations.map {
      combinationElems =>
        val dataPatterns = combinationElems.map(functionDslWithComplexity(_)).toList
        val elemsComplexity = combinationElems.map(functionDslWithComplexity(_).complexity).toSeq
        QueryWithComplexity(
          reduceMultipleDataPatterns(dataPatterns),
          DataPatternComplexity.functionCombination(elemsComplexity)
        )
    }
    val reducedQueriesWithComplexity = reduceRepeatedQueries(queriesWithComplexity)
    log.info(s"${reducedQueriesWithComplexity.size} queries have been generated")
    reducedQueriesWithComplexity
  }

  private def reduceRepeatedQueries(queriesWithComplexity: List[QueryWithComplexity]): List[QueryWithComplexity] =
    queriesWithComplexity.groupBy(_.query).values.map(_.head).toList

  def getAllSetCombinations(ls: List[Set[String]]): List[Set[String]] = (2 to ls.size)
    .flatMap(size => ls.combinations(size).flatMap(getAllCombinations))
    .toList

  def setCombinations(l1: Set[String], l2: Set[String]): List[Set[String]] =
    l1.flatMap(x => l2.map(y => Set(x, y))).toList

  private def getAllCombinations(lists: List[Set[String]]): List[Set[String]] =
    lists match {
      case Nil => List(Set.empty[String])
      case head :: tail =>
        val output = for {
          x <- head
          xs <- DataPatternInference.getAllCombinations(tail)
        } yield xs ++ Set(x)
        output.toList
    }

  private def checkIsIn(dataPattern: DataPattern): Boolean =
    dataPattern.query.startsWith(dslfunctions.inKeyword) ||
      (dataPattern.query.startsWith(dslfunctions.columnKeyword) &&
        InferenceUtils.isColWithStringLiteral(dataPattern.maybeDatumFunction))

  def reduceIsInFunction(dataPatterns: List[DataPattern]): List[DataPattern] =
    dataPatterns.find(checkIsIn) match {
      // Contains an in function
      case Some(oldIsIn: DataPattern) =>
        val (isInDataPatterns, otherDataPatterns) = dataPatterns.partition(checkIsIn)
        val combinedSetOfValues = isInDataPatterns.foldLeft(Set.empty[String]) {
          (setOfValues, dataPattern) =>
            setOfValues ++ {
              InferenceUtils.extractIsInValues(dataPattern.maybeDatumFunction).toList match {
                case Nil => InferenceUtils
                  .extractLiteralValue(dataPattern.maybeDatumFunction)
                  .map(Set.apply(_))
                  .getOrElse(Set.empty)
                case values => values.toSet
              }
            }
        }
        // Re-evaluate in set of values
        val filteredSetOfValues = combinedSetOfValues.filterNot {
          value =>
            otherDataPatterns.exists {
              dataPattern => InferenceUtils.containsIsInValues(dataPattern.maybeDatumFunction, value)
            }
        }
        if (filteredSetOfValues.isEmpty) { return otherDataPatterns }
        val (query, complexity) = filteredSetOfValues.toList match {
          case head :: Nil =>
            val query = dslfunctions
              .buildQuery(dslfunctions.columnKeyword, Comparator.eq.toString, dslfunctions.literal(head))
            val complexity = DataPatternComplexity.NoParamFunction
            (query, complexity)
          case _ =>
            val query = dslfunctions.in(filteredSetOfValues)
            val complexity = oldIsIn.complexity
            (query, complexity)
        }
        val newIsIn = DataPattern(oldIsIn.id, oldIsIn.name, query, complexity)
        otherDataPatterns :+ newIsIn

      // Does not contain an isIn function
      case _ => dataPatterns
    }

  private def checkIsRlike(dataPattern: DataPattern): Boolean = dataPattern.query.startsWith(dslfunctions.rLikeKeyword)

  private def reduceRegexFunction(dataPatterns: List[DataPattern]): List[DataPattern] =
    dataPatterns.find(checkIsRlike) match {
      // Contains an isIn function
      case Some(oldRlike: DataPattern) =>
        val (rLikeDataPatterns, otherDataPatterns) = dataPatterns.partition(checkIsRlike)
        val regexes = rLikeDataPatterns.flatMap(
          rLikeDataPattern =>
            InferenceUtils
              .extractRLikeRegex(rLikeDataPattern.maybeDatumFunction)
              .map(regex => regex.replaceAll("\\\\", "\\\\\\\\"))
        )
        val combinedRegex = RegexUtils.combineStringRegex(regexes)
        val newRLikeQuery = dslfunctions.rLike(combinedRegex)
        val newRLike = DataPattern(oldRlike.id, oldRlike.name, newRLikeQuery, oldRlike.complexity)
        otherDataPatterns :+ newRLike
      case _ => dataPatterns
    }

  private def reduceDatumAndColumnFunction(dataPatterns: List[DataPattern]): (List[DataPattern], String) = {
    val colNameRLikeRegexes = dataPatterns.flatMap(
      dataPattern =>
        dataPattern.maybeColumnFunction match {
          case Some(colNameRLike: ColNameRLike) => Some(colNameRLike.regex.replaceAll("\\\\", "\\\\\\\\"))
          case _                                => None
        }
    )

    val reducedDataPatterns = dataPatterns.map {
      dataPattern =>
        if (dataPattern.query.contains(dslfunctions.separatorKeyword)) {
          val newQuery = dataPattern.query.split(dslfunctions.separatorKeyword + dslfunctions.colNameRLikeKeyword).head
          dataPattern.copy(query = newQuery)
        } else { dataPattern }
    }

    val combinedColNameRLikeRegex = RegexUtils.combineStringRegex(colNameRLikeRegexes)
    (reducedDataPatterns, combinedColNameRLikeRegex)
  }

  private def reduceRepeatedPatterns(dataPatterns: List[DataPattern]): List[DataPattern] =
    dataPatterns.groupBy(_.query).values.map(_.head).toList

}
