package org.example.scanner.dsl

import akka.event.slf4j.SLF4JLogging
import org.example.scanner.dsl.ast.ArithmeticOperator.ArithmeticOperatorType
import org.example.scanner.dsl.ast.Comparator.ComparatorType
import org.example.scanner.dsl.ast.{ArithmeticOperator, Comparator, ErrorHandling}
import org.example.scanner.impl.column.ColNameRLikeMode
import org.example.scanner.impl.config.ExternalJarLoader
import org.example.scanner.impl.evaluation.{CountryCode, FileExtension}
import org.example.scanner.impl.utils.FileUtils.cleanString
import org.example.scanner.impl.{dslfunctions, functions}

import scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers}
import org.example.scanner.sdk.traits.dsl
import org.example.scanner.sdk.traits.dsl.{DatumFunction, EvaluationFunction}
import org.example.scanner.sdk.types.ScannerTypes

import scala.collection.immutable.HashSet
import scala.language.postfixOps
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf
import scala.util.parsing.combinator._
import scala.util.Try

object DSLParser extends JavaTokenParsers with PackratParsers with SLF4JLogging {

  private var DEBUG_ENABLED = false
  private def logParser[T](x: Parser[T])(msg: String): Parser[T] = if (DEBUG_ENABLED) log(x)(msg) else x
  private lazy val regexParser: Parser[String] = logParser(stringLiteral)("regexParser")

  private lazy val columnFunction: Parser[ast.ColumnFunction] = logParser {
    dslfunctions.colNameRLikeKeyword ~> ("(" ~> regexParser ~ ("," ~> ident ?) <~ ")") ^?
      (
        {
          case regex ~ Some(mode) if ColNameRLikeMode.isValidCode(mode) =>
            functions.colNameRLike(cleanString(regex), mode)
          case regex ~ None => functions.colNameRLike(cleanString(regex))
        },
        { case _ ~ input => ErrorHandling.invalidArgumentError(input, ErrorHandling.ColNameRLikeModeArgumentError) }
      )
  }("columnFunction")

  private lazy val comparationOperator: Parser[ComparatorType] = logParser {
    (Comparator.eqq.toString | Comparator.eq.toString | Comparator.gte.toString | Comparator.lte.toString |
      Comparator.gt.toString | Comparator.lt.toString) ^? {
      case x if ast.Comparator.exists(x) => ast.Comparator.findValue(x).get
    }
  }("comparationOperator")

  private lazy val level1ArithmeticOperator: Parser[ArithmeticOperatorType] = logParser {
    (ArithmeticOperator.mul.toString | ArithmeticOperator.div.toString | ArithmeticOperator.mod.toString) ^? {
      case x if ast.ArithmeticOperator.exists(x) => ast.ArithmeticOperator.findValue(x).get
    }
  }("level1ArithmeticOperator")

  private lazy val level2ArithmeticOperator: Parser[ArithmeticOperatorType] = logParser {
    (ArithmeticOperator.sum.toString | ArithmeticOperator.sub.toString) ^? {
      case x if ast.ArithmeticOperator.exists(x) => ast.ArithmeticOperator.findValue(x).get
    }
  }("level2ArithmeticOperator")

  private lazy val literalParser: Parser[ast.TransformationFunction] = logParser {
    floatingPointNumber ^^ (strNumber => functions.literal(BigDecimal(strNumber))) |
      "true" ^^^ functions.literal(true) | "false" ^^^ functions.literal(false) |
      stringLiteral ^^
        (
          str => {
            // Remove front and trailing "
            val cleanStr = str.drop(1).dropRight(1)
            functions.literal(cleanStr)
          }
          )
  }("literalParser")

  private lazy val booleanParser: Parser[String] = "(?i)true|false".r

  private lazy val customFunctionsParser: Parser[EvaluationFunction] = ExternalJarLoader
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
          val mirror = universe.runtimeMirror(classLoader)

          val classSymbol = mirror.classSymbol(clazz)
          // Access the primary constructor
          val primaryConstructor = classSymbol.primaryConstructor.asMethod
          // Get the list of parameters
          val params = primaryConstructor.paramLists.flatten

          // Get the companion object
          val companionSymbol = mirror.classSymbol(clazz).companion.asModule
          val companionObject = mirror.reflectModule(companionSymbol).instance

          if (params.isEmpty) {
            // Access and invoke a function 'noParamDSL' from the companion object
            val myFunctionMethod = companionObject.getClass.getMethod("noParamDSL")

            myFunctionMethod.invoke(companionObject).asInstanceOf[String] ^^^
              clazz.getDeclaredConstructor().newInstance().asInstanceOf[EvaluationFunction]
          } else {
            val paramParsers = params.map(
              param =>
                param.typeSignature match {
                  case x if x =:= typeOf[String]  => stringLiteral ^^ cleanString
                  case x if x =:= typeOf[Double]  => floatingPointNumber ^^ { _.toDouble }
                  case x if x =:= typeOf[Int]     => wholeNumber ^^ { _.toInt }
                  case x if x =:= typeOf[Boolean] => booleanParser ^^ { _.toBoolean }
                  case _                          => failure("Unsupported parameter type")
                }
            )

            def flattenResult(result: Any): List[Any] =
              result match {
                case a ~ b     => flattenResult(a) ::: List(b)
                case nonNested => List(nonNested)
              }

            val combinedParser = paramParsers match {
              case Nil => failure("No parameters found")
              case parsers =>
                parsers.reduceLeft[Parser[Any]]((acc, p) => acc ~ ("," ~> p)) ^^ { result => flattenResult(result) }
            }

            val keywordField = companionObject.getClass.getMethod("keyword")
            val keywordValue = keywordField.invoke(companionObject).asInstanceOf[String]

            keywordValue ~> ("(" ~> combinedParser <~ ")") ^^ {
              params =>
                val constructorMirror = mirror.reflectClass(classSymbol).reflectConstructor(primaryConstructor)
                constructorMirror(params: _*).asInstanceOf[EvaluationFunction]
            }
          }
        } match {
          case util.Failure(exception) =>
            log.warn(s"Error trying to instantiate $className", exception)
            None
          case util.Success(value) =>
            log.info(s"Successfully created class $className")
            Some(value)
        }
    } match {
    case Nil           => failure("No custom classes to load")
    case parser :: Nil => parser
    case parsers       => parsers.reduceLeft(_ | _)
  }

  private lazy val transformationFunctionParser: PackratParser[ast.TransformationFunction] = logParser {
    ((transformationFunctionParser ~ level2ArithmeticOperator) ~ transformationFunctionParser |
      (transformationFunctionParser ~ level1ArithmeticOperator) ~ transformationFunctionParser) ^^ {
      case lhs ~ operator ~ rhs =>
        log.debug(f"[transformationFunctionParser] with [$operator] and lhs[$lhs] and rhs[$rhs]")
        functions.arithmeticFunction(lhs, operator, rhs)
    } | "(" ~> transformationFunctionParser <~ ")" |
      dslfunctions.lowerKeyword ~>
        ("(" ~> transformationFunctionParser <~ ")") ^^
        (func => functions.lower(func)) |
      dslfunctions.upperKeyword ~>
        ("(" ~> transformationFunctionParser <~ ")") ^^
        (func => functions.upper(func)) |
      dslfunctions.replaceKeyword ~>
        ("(" ~> transformationFunctionParser ~ ("," ~> regexParser) ~ ("," ~> stringLiteral) <~ ")") ^^ {
        case func ~ regex ~ replacement => functions.replace(func, cleanString(regex), cleanString(replacement))
      } |
      dslfunctions.trimKeyword ~>
        ("(" ~> transformationFunctionParser <~ ")") ^^
        (func => functions.trim(func)) |
      dslfunctions.lengthKeyword ~>
        ("(" ~> transformationFunctionParser <~ ")") ^^
        (func => functions.length(func)) | dslfunctions.age ^^^ functions.age |
      dslfunctions.columnKeyword ^^^ functions.column | literalParser
  }("transformationFunctionParser")



  private lazy val comparationParser: Parser[EvaluationFunction] = logParser {
    (transformationFunctionParser ~ comparationOperator) ~ transformationFunctionParser ^^ {
      case lhs ~ comparator ~ rhs =>
        log.debug(f"[comparationParser] with [$comparator] and lhs[$lhs] and rhs[$rhs]")
        functions.comparation(lhs, comparator, rhs)
    }
  }("comparationParser")

  private lazy val containsNameParser: Parser[String] = logParser(ident ^? {
    case key if dslfunctions.validContainsKey(key) => key
  })("containsNameParser") withFailureMessage (ErrorHandling.InvalidFunctionError)

  private lazy val containsParser: Parser[EvaluationFunction] = logParser {
    containsNameParser ~ ("(" ~> (floatingPointNumber ?) <~ ")") ^?
      (
        {
          case key ~ threshold if threshold.getOrElse("1").toDouble > 0 && threshold.getOrElse("1").toDouble <= 1 =>
            functions.containsKey(key, threshold.map(e => e.toDouble))
        },
        { _ => s"${ErrorHandling.InvalidIsInThresholdError}" }
      )
  }("containsParser")

  private lazy val evaluationFunctionParser: Parser[EvaluationFunction] = logParser {
    dslfunctions.rLikeKeyword ~>
      ("(" ~> regexParser <~ ")") ^^
      (
        r => {
          val clean = cleanString(r)
          functions.rLike(clean)
        }
        ) | dslfunctions.email ^^^ functions.email | dslfunctions.imei ^^^ functions.imei |
      dslfunctions.creditCard ^^^ functions.creditCard | dslfunctions.ipAddress ^^^ functions.ipAddress |
      dslfunctions.personName ^^ { (_: String) => functions.personName } |
      dslfunctions.location ^^ { (_: String) => functions.location } |
      dslfunctions.organization ^^ { (_: String) => functions.organization } |
      dslfunctions.engineNumber ^^ { (_: String) => functions.engineNumber } |
      dslfunctions.rewardPlan ^^ { (_: String) => functions.rewardPlan } |
      dslfunctions.occupationNLP ^^ { (_: String) => functions.occupationNLP } |
      dslfunctions.username ^^ { (_: String) => functions.username } |
      dslfunctions.surname ^^ { (_: String) => functions.surname } |
      dslfunctions.name ^^ { (_: String) => functions.name } | dslfunctions.booleanSet ^^^ functions.booleanSet |
      dslfunctions.date ^^^ functions.date | dslfunctions.timestamp ^^^ functions.timestamp |
      dslfunctions.geolocation ^^^ functions.geolocation | dslfunctions.image ^^^ functions.image |
      dslfunctions.video ^^^ functions.video | dslfunctions.audio ^^^ functions.audio |
      dslfunctions.uuid ^^^ functions.uuid | dslfunctions.url ^^^ functions.url | dslfunctions.uri ^^^ functions.uri |
      customFunctionsParser |
      dslfunctions.accountNumberKeyword ~>
        ("(" ~> (stringLiteral ?) <~ ")") ^?
        (
          {
            case Some(cCode: String) if CountryCode.isValidCode(cleanString(cCode)) =>
              functions.accountNumber(CountryCode.findValue(cleanString(cCode)))
            case None => functions.accountNumber
          },
          { input => s"${ErrorHandling.invalidArgumentError(input, ErrorHandling.CountryFunctionArgumentError)}" }
        ) |
      dslfunctions.passportNumberKeyword ~>
        ("(" ~> (stringLiteral ?) <~ ")") ^?
        (
          {
            case Some(cCode: String) if CountryCode.isValidCode(cleanString(cCode)) =>
              functions.passportNumber(CountryCode.findValue(cleanString(cCode)))
            case None => functions.passportNumber
          },
          { input => s"${ErrorHandling.invalidArgumentError(input, ErrorHandling.CountryFunctionArgumentError)}" }
        ) |
      dslfunctions.socialSecurityNumberKeyword ~>
        ("(" ~> (stringLiteral ?) <~ ")") ^?
        (
          {
            case Some(cCode: String) if CountryCode.isValidCode(cleanString(cCode)) =>
              functions.socialSecurityNumber(CountryCode.findValue(cleanString(cCode)))
            case None => functions.socialSecurityNumber
          },
          { input => s"${ErrorHandling.invalidArgumentError(input, ErrorHandling.CountryFunctionArgumentError)}" }
        ) |
      dslfunctions.phoneNumberKeyword ~>
        ("(" ~> (stringLiteral ?) <~ ")") ^?
        (
          {
            case Some(cCode: String) if CountryCode.isValidCode(cleanString(cCode)) =>
              functions.phoneNumber(CountryCode.findValue(cleanString(cCode)))
            case None => functions.phoneNumber
          },
          { input => s"${ErrorHandling.invalidArgumentError(input, ErrorHandling.CountryFunctionArgumentError)}" }
        ) |
      dslfunctions.residencePermitKeyword ~>
        ("(" ~> (stringLiteral ?) <~ ")") ^?
        (
          {
            case Some(cCode: String) if CountryCode.isValidCode(cleanString(cCode)) =>
              functions.residencePermit(CountryCode.findValue(cleanString(cCode)))
            case None => functions.residencePermit
          },
          { input => s"${ErrorHandling.invalidArgumentError(input, ErrorHandling.CountryFunctionArgumentError)}" }
        ) |
      dslfunctions.taxIdentifierKeyword ~>
        ("(" ~> (stringLiteral ?) <~ ")") ^?
        (
          {
            case Some(cCode: String) if CountryCode.isValidCode(cleanString(cCode)) =>
              functions.taxIdentifier(CountryCode.findValue(cleanString(cCode)))
            case None => functions.taxIdentifier
          },
          { input => s"${ErrorHandling.invalidArgumentError(input, ErrorHandling.CountryFunctionArgumentError)}" }
        ) |
      dslfunctions.identificationNumberKeyword ~>
        ("(" ~> (stringLiteral ?) <~ ")") ^?
        (
          {
            case Some(cCode: String) if CountryCode.isValidCode(cleanString(cCode)) =>
              functions.identificationNumber(CountryCode.findValue(cleanString(cCode)))
            case None => functions.identificationNumber
          },
          { input => s"${ErrorHandling.invalidArgumentError(input, ErrorHandling.CountryFunctionArgumentError)}" }
        ) |
      dslfunctions.drivingLicenseKeyword ~>
        ("(" ~> (stringLiteral ?) <~ ")") ^?
        (
          {
            case Some(cCode: String) if CountryCode.isValidCode(cleanString(cCode)) =>
              functions.drivingLicense(CountryCode.findValue(cleanString(cCode)))
            case None => functions.drivingLicense
          },
          { input => s"${ErrorHandling.invalidArgumentError(input, ErrorHandling.CountryFunctionArgumentError)}" }
        ) |
      dslfunctions.documentKeyword ~>
        ("(" ~> (rep1sep(stringLiteral, ",") ?) <~ ")") ^?
        (
          {
            case Some(fileExtensions: List[String])
              if fileExtensions
                .forall(fileExtension => FileExtension.isValidFileExtension(cleanString(fileExtension))) =>
              functions
                .document(fileExtensions.map(fileExtension => FileExtension.findValue(cleanString(fileExtension)).get))
            case None => functions.document
          },
          {
            input =>
              val wrongFileExtension = input
                .get
                .find(fileExtension => !FileExtension.isValidFileExtension(cleanString(fileExtension)))
              s"${ErrorHandling.invalidArgumentError(wrongFileExtension, ErrorHandling.FileExtensionArgumentError)}"
          }
        ) |
      dslfunctions.inKeyword ~>
        ("(" ~> rep1sep(stringLiteral, ",") ~ ("," ~> floatingPointNumber ?) <~ ")") ^?
        (
          {
            case strValues ~ maybeThreshold
              if maybeThreshold.getOrElse("1").toDouble > 0 && maybeThreshold.getOrElse("1").toDouble <= 1 =>
              val cleanValues = strValues.map(cleanString).to[HashSet]
              functions.in(cleanValues, maybeThreshold.map(e => e.toDouble))
          },
          { p => ErrorHandling.InvalidIsInThresholdError }
        ) |
      dslfunctions.inFileKeyword ~>
        ("(" ~> stringLiteral ~ ("," ~> floatingPointNumber ?) <~ ")") ^?
        (
          {
            case filename ~ maybeThreshold
              if maybeThreshold.getOrElse("1").toDouble > 0 && maybeThreshold.getOrElse("1").toDouble <= 1 =>
              val cleanFilename = cleanString(filename)
              functions.inFile(cleanFilename, maybeThreshold.map(e => e.toDouble))
          },
          { p => ErrorHandling.InvalidIsInThresholdError }
        ) |
      dslfunctions.containsKeyword ~>
        ("(" ~> rep1sep(stringLiteral, ",") ~ ("," ~> booleanParser ?) <~ ")") ^? {
        case strValues ~ maybeCaseSensitive =>
          val cleanValues = strValues.map(cleanString)
          functions.contains(cleanValues, maybeCaseSensitive.getOrElse("true").toBoolean)
      } |
      dslfunctions.containsWordsKeyword ~>
        ("(" ~> rep1sep(stringLiteral, ",") ~ ("," ~> booleanParser ?) <~ ")") ^? {
        case strValues ~ maybeCaseSensitive =>
          val cleanValues = strValues.map(cleanString)
          functions.containsWords(cleanValues, maybeCaseSensitive.getOrElse("true").toBoolean)
      } |
      dslfunctions.fileContainsKeyword ~>
        ("(" ~> stringLiteral ~ ("," ~> booleanParser ?) <~ ")") ^? {
        case filename ~ maybeCaseSensitive =>
          val cleanFilename = cleanString(filename)
          functions.fileContains(cleanFilename, maybeCaseSensitive.getOrElse("true").toBoolean)
      } |
      dslfunctions.fileContainsWordsKeyword ~>
        ("(" ~> stringLiteral ~ ("," ~> booleanParser ?) <~ ")") ^? {
        case filename ~ maybeCaseSensitive =>
          val cleanFilename = cleanString(filename)
          functions.fileContainsWords(cleanFilename, maybeCaseSensitive.getOrElse("true").toBoolean)
      } |
      dslfunctions.betweenKeyword ~>
        ("(" ~> transformationFunctionParser ~ ("," ~> floatingPointNumber) ~ ("," ~> floatingPointNumber) <~ ")") ^^ {
        case func ~ minValue ~ maxValue => functions.between(func, minValue.toDouble, maxValue.toDouble)
      } |
      dslfunctions.distributionKeyword ~>
        ("(" ~> transformationFunctionParser ~ ("," ~> floatingPointNumber) ~ ("," ~> floatingPointNumber) <~ ")") ^^ {
        case func ~ meanValue ~ stdValue => functions.distribution(func, meanValue.toDouble, stdValue.toDouble)
      } |
      dslfunctions.dataTypeKeyword ~>
        ("(" ~> stringLiteral <~ ")") ^?
        (
          {
            case dataType if ScannerTypes.getScannerTypeFromName(cleanString(dataType)) != null =>
              val scannerType = ScannerTypes.getScannerTypeFromName(cleanString(dataType))
              functions.dataType(scannerType)
          },
          { p => ErrorHandling.InvalidDataType }
        ) |
      dslfunctions.castableKeyword ~>
        ("(" ~> stringLiteral <~ ")") ^?
        (
          {
            case dataType if ScannerTypes.getScannerTypeFromName(cleanString(dataType)) != null =>
              val scannerType = ScannerTypes.getScannerTypeFromName(cleanString(dataType))
              functions.castable(scannerType)
          },
          { p => ErrorHandling.InvalidDataType }
        ) | containsParser
  }("evaluationFunctionParser")

  // DatumFunction es el nivel superior de la DSL
  private lazy val datumFunction: PackratParser[DatumFunction] = logParser {
    (datumFunction <~ dslfunctions.andKeyword) ~ datumFunction ^^ {
      case lhs ~ rhs =>
        log.debug(f"[datumFunction] with [&] and lhs[$lhs] and rhs[$rhs]")
        functions.andOperator(lhs, rhs)
    } |
      (datumFunction <~ dslfunctions.orKeyword) ~ datumFunction ^^ {
        case lhs ~ rhs =>
          log.debug(f"[datumFunction] with [|] and lhs[$lhs] and rhs[$rhs]")
          functions.orOperator(lhs, rhs)
      } |
      dslfunctions.notKeyword ~>
        (evaluationFunctionParser | comparationParser | "(" ~> datumFunction <~ ")" | datumFunction) ^^ {
        function =>
          log.debug(f"[datumFunction] with [!] and function[$function]")
          functions.notOperator(function)
      } | "(" ~> datumFunction <~ ")" | evaluationFunctionParser | comparationParser
  }("datumFunction")

  // Evaluamos [[DatumFunction]] que van solas
  private lazy val enhancedDatumFunction: Parser[dsl.DataPatternExpression] = logParser {
    dslfunctions.birthDate ^^^ parseDataPatternExpression(dslfunctions.birthDateEnhanced) |
      dslfunctions.socialUrl ^^^ parseDataPatternExpression(dslfunctions.socialUrlEnhanced) |
      dslfunctions.accountNumber ^^^ parseDataPatternExpression(dslfunctions.accountNumberEnhanced) |
      dslfunctions.taxIdentifier ^^^ parseDataPatternExpression(dslfunctions.taxIdentifierEnhanced) |
      dslfunctions.phoneNumber ^^^ parseDataPatternExpression(dslfunctions.phoneNumberEnhanced) |
      dslfunctions.passportNumber ^^^ parseDataPatternExpression(dslfunctions.passportNumberEnhanced) |
      dslfunctions.socialSecurityNumber ^^^ parseDataPatternExpression(dslfunctions.socialSecurityNumberEnhanced) |
      dslfunctions.creditCard ^^^ parseDataPatternExpression(dslfunctions.creditCardEnhanced) |
      dslfunctions.imei ^^^ parseDataPatternExpression(dslfunctions.imeiEnhanced) |
      dslfunctions.residencePermit ^^^ parseDataPatternExpression(dslfunctions.residencePermitEnhanced) |
      dslfunctions.identificationNumber ^^^ parseDataPatternExpression(dslfunctions.identificationNumberEnhanced) |
      dslfunctions.ipAddress ^^^ parseDataPatternExpression(dslfunctions.ipAddressEnhanced) |
      dslfunctions.drivingLicense ^^^ parseDataPatternExpression(dslfunctions.drivingLicenseEnhanced) |
      dslfunctions.geolocation ^^^ parseDataPatternExpression(dslfunctions.geolocationEnhanced) |
      dslfunctions.document ^^^ parseDataPatternExpression(dslfunctions.documentEnhanced) |
      dslfunctions.image ^^^ parseDataPatternExpression(dslfunctions.imageEnhanced) |
      dslfunctions.video ^^^ parseDataPatternExpression(dslfunctions.videoEnhanced) |
      dslfunctions.audio ^^^ parseDataPatternExpression(dslfunctions.audioEnhanced) |
      dslfunctions.uuid ^^^ parseDataPatternExpression(dslfunctions.uuidEnhanced) |
      dslfunctions.url ^^^ parseDataPatternExpression(dslfunctions.urlEnhanced) |
      dslfunctions.uri ^^^ parseDataPatternExpression(dslfunctions.uriEnhanced)

  }("enhancedDatumFunction")

  private lazy val dataPatternExpressionParser: Parser[dsl.DataPatternExpression] = logParser {
    (datumFunction <~ dslfunctions.separatorKeyword) ~ columnFunction ^^ {
      case datumFunction ~ columnFunction =>
        log.debug(
          f"[dataPatternExpressionParser] with [;] and datumFunction[$datumFunction] and columnFunction[$columnFunction]"
        )
        ast.DatumAndColumnFunction(datumFunction, columnFunction)
    } | enhancedDatumFunction <~ (not(dslfunctions.andKeyword) ~ not(dslfunctions.orKeyword)) | columnFunction ^^ {
      columnFunction => ast.DatumAndColumnFunction(null, columnFunction)
    } | datumFunction
  }("dataPatternExpressionParser")


  private[dsl] def parseDSL(str: String): ParseResult[dsl.DataPatternExpression] = {
    log.debug(f"Parsing $str")
    parseAll(dataPatternExpressionParser, str)
  }
  def parseDataPatternExpression(str: String): dsl.DataPatternExpression = {
    log.debug(s"parseDataPatternExpression ${str}")
    parseDSL(str) match {
      case Success(p, _) => p
      case Failure(msg, x) =>
        throw new DSLParserFailure(f"Source: ${x.source.subSequence(0, x.pos.column - 1)} <---\nFailure: $msg")
      case Error(msg, _) => throw new DSLParserError(msg)
    }
  }
}

private[dsl] class DSLParserFailure(msg: String) extends Exception(msg)
private[dsl] class DSLParserError(msg: String) extends Exception(msg)
