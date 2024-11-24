package org.example.scanner.impl

import org.example.scanner.dsl.ast.ArithmeticOperator.ArithmeticOperatorType
import org.example.scanner.dsl.ast.Comparator.ComparatorType
import org.example.scanner.dsl.ast.{ColumnFunction, DatumAndColumnFunction, TransformationFunction}
import org.example.scanner.impl.column.ColNameRLike
import org.example.scanner.impl.column.ColNameRLikeMode.ColNameRLikeModeType
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.FileExtension.FileExtensionType
import org.example.scanner.impl.evaluation._
import org.example.scanner.impl.evaluation.traits.dsl.ContainsDSL
import org.example.scanner.impl.transformation._
import org.example.scanner.sdk.traits.dsl.{DatumFunction, EvaluationFunction}
import org.example.scanner.sdk.types.ScannerType

import scala.collection.immutable.HashSet

object functions {

  def datumAndColumnFunction(datumFunction: DatumFunction, columnFunction: ColumnFunction): DatumAndColumnFunction =
    DatumAndColumnFunction(datumFunction, columnFunction)

  def containsKey(key: String, maybeThreshold: Option[Double]): EvaluationFunction =
    ContainsDSL.getDatumFunction(key, maybeThreshold)

  def colNameRLike(regex: String): ColNameRLike = ColNameRLike(regex)
  def colNameRLike(regex: String, mode: ColNameRLikeModeType): ColNameRLike = ColNameRLike(regex, mode)
  def colNameRLike(regex: String, modeStr: String): ColNameRLike = ColNameRLike(regex, modeStr)
  def orOperator(lhs: DatumFunction, rhs: DatumFunction): OrOperator = OrOperator(lhs, rhs)
  def andOperator(lhs: DatumFunction, rhs: DatumFunction): AndOperator = AndOperator(lhs, rhs)
  def notOperator(function: DatumFunction): NotOperator = NotOperator(function)
  def between(func: TransformationFunction, min: Double, max: Double): Between = Between.between(func, min, max)
  def age: Age = Age.age

  def arithmeticFunction(
                          lhs: TransformationFunction,
                          operator: ArithmeticOperatorType,
                          rhs: TransformationFunction
                        ): ArithmeticFunction = ArithmeticFunction(lhs, operator, rhs)
  def passportNumber: PassportNumber = PassportNumber.noParamFunction

  def passportNumber(maybeCountryCode: Option[CountryCodeType]): PassportNumber =
    PassportNumber.countryCodeFunction(maybeCountryCode)

  def socialSecurityNumber: SocialSecurityNumber = SocialSecurityNumber.noParamFunction

  def residencePermit: ResidencePermit = ResidencePermit.noParamFunction

  def residencePermit(maybeCountryCode: Option[CountryCodeType]): ResidencePermit =
    ResidencePermit.countryCodeFunction(maybeCountryCode)

  def taxIdentifier: TaxIdentifier = TaxIdentifier.noParamFunction

  def taxIdentifier(maybeCountryCode: Option[CountryCodeType]): TaxIdentifier =
    TaxIdentifier.countryCodeFunction(maybeCountryCode)

  def inFile(filename: String): In = InFile.noLevenshteinFunction(filename)

  def inFile(filename: String, maybeThreshold: Option[Double]): In =
    InFile.levenshteinFunction(filename, maybeThreshold)

  def containsWords(listOfValues: Seq[String]): ContainsWords = ContainsWords.apply(listOfValues)

  def containsWords(listOfValues: Seq[String], caseSensitive: Boolean): ContainsWords =
    ContainsWords.apply(listOfValues, caseSensitive)

  def document: Document = Document.noParamFunction

  def document(fileExtensions: Seq[FileExtensionType]): Document = Document.fileExtensionsFunction(fileExtensions)

  def in(listOfValues: HashSet[String]): In = In.apply(listOfValues)

  def in(listOfValues: HashSet[String], maybeThreshold: Option[Double]): In = In.apply(listOfValues, maybeThreshold)

  def drivingLicense: DrivingLicense = DrivingLicense.noParamFunction

  def drivingLicense(maybeCountryCode: Option[CountryCodeType]): DrivingLicense =
    DrivingLicense.countryCodeFunction(maybeCountryCode)

  def identificationNumber: IdentificationNumber = IdentificationNumber.noParamFunction

  def identificationNumber(maybeCountryCode: Option[CountryCodeType]): IdentificationNumber =
    IdentificationNumber.countryCodeFunction(maybeCountryCode)

  def distribution(func: TransformationFunction, mean: Double, std: Double): InlierNormalDist =
    InlierNormalDist.distribution(func, mean, std)

  def dataType(dataType: ScannerType): DataType = DataType.apply(dataType)

  def castable(dataType: ScannerType): Castable = Castable.apply(dataType)

  def fileContains(filename: String, caseSensitive: Boolean): Contains =
    FileContains.fileContainsFunction(filename, caseSensitive)

  def fileContains(filename: String): Contains = FileContains.caseSensitiveFileContainsFunction(filename)

  def fileContainsWords(filename: String, caseSensitive: Boolean): ContainsWords =
    FileContainsWords.fileContainsWordsFunction(filename, caseSensitive)

  def fileContainsWords(filename: String): ContainsWords =
    FileContainsWords.caseSensitiveFileContainsWordsFunction(filename)

  def socialSecurityNumber(maybeCountryCode: Option[CountryCodeType]): SocialSecurityNumber =
    SocialSecurityNumber.countryCodeFunction(maybeCountryCode)

  def phoneNumber: PhoneNumber = PhoneNumber.noParamFunction

  def phoneNumberEnhanced: DatumAndColumnFunction = PhoneNumber.enhancedFunction

  def phoneNumber(maybeCountryCode: Option[CountryCodeType]): PhoneNumber =
    PhoneNumber.countryCodeFunction(maybeCountryCode)

  def accountNumber: AccountNumber = AccountNumber.noParamFunction

  def accountNumber(maybeCountryCode: Option[CountryCodeType]): AccountNumber =
    AccountNumber.countryCodeFunction(maybeCountryCode)

  def contains(listOfValues: Seq[String]): Contains = Contains.apply(listOfValues)

  def contains(listOfValues: Seq[String], caseSensitive: Boolean): Contains =
    Contains.apply(listOfValues, caseSensitive)

  def lower(func: TransformationFunction): Lower = Lower.lower(func)

  def upper(func: TransformationFunction): Upper = Upper.upper(func)

  def replace(func: TransformationFunction, regex: String, replacement: String): Replace =
    Replace.replace(func, regex, replacement)

  def trim(func: TransformationFunction): Trim = Trim.trim(func)

  def length(func: TransformationFunction): Length = Length(func)

  def column: Column = Column.column

  def literal(value: Any): Literal = Literal(value)

  def comparation(lhs: TransformationFunction, comparator: ComparatorType, rhs: TransformationFunction): Comparison =
    evaluation.Comparison(lhs, comparator, rhs)

  def rLike(regex: String): RLike = RLike.apply(regex)

  def email: Email = Email.noParamFunction

  def imei: IMEI = IMEI.noParamFunction

  def creditCard: CreditCard = CreditCard.noParamFunction

  def ipAddress: IpAddress = IpAddress.noParamFunction

  def personName: PersonName = PersonName.noParamFunction

  def location: Location = Location.noParamFunction

  def organization: Organization = Organization.noParamFunction

  def engineNumber: EngineNumber = EngineNumber.noParamFunction

  def rewardPlan: RewardPlan = RewardPlan.noParamFunction

  def occupationNLP: OccupationNLP = OccupationNLP.noParamFunction

  def username: Username = Username.noParamFunction

  def surname: Surname = Surname.noParamFunction

  def name: Name = Name.noParamFunction

  def booleanSet: BooleanSet = BooleanSet.noParamFunction

  def date: Date = Date.noParamFunction

  def timestamp: Timestamp = Timestamp.noParamFunction

  def geolocation: Geolocation = Geolocation.noParamFunction

  def image: Image = Image.noParamFunction

  def video: Video = Video.noParamFunction

  def audio: Audio = Audio.noParamFunction

  def uuid: UUID = UUID.noParamFunction

  def url: URL = URL.noParamFunction

  def uri: URI = URI.noParamFunction

}
