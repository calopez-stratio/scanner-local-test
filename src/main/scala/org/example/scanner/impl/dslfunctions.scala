package org.example.scanner.impl

import org.example.scanner.impl.column.{ColNameRLike, ColNameRLikeMode}
import org.example.scanner.impl.column.ColNameRLikeMode.ColNameRLikeModeType
import org.example.scanner.impl.evaluation.CountryCode.CountryCodeType
import org.example.scanner.impl.evaluation.FileExtension.FileExtensionType
import org.example.scanner.impl.evaluation._
import org.example.scanner.impl.evaluation.traits.dsl.ContainsDSL
import org.example.scanner.impl.transformation._
import org.example.scanner.sdk.traits.impl.FunctionDSL

object dslfunctions {
  val separatorKeyword: String = ";"

  def fileContainsKeyword: String = FileContains.keyword

  def fileContains(filename: String): FunctionDSL = FileContains.caseSensitiveFileContainsDSL(filename)

  def fileContains(filename: String, caseSensitive: Boolean): FunctionDSL =
    FileContains.fileContainsDSL(filename, caseSensitive: Boolean)

  def fileContainsWordsKeyword: String = FileContainsWords.keyword

  def betweenKeyword: String = Between.keyword

  def dataTypeKeyword: String = DataType.keyword

  def containsWordsKeyword: String = ContainsWords.keyword

  def containsKeyword: String = Contains.keyword

  def inFileKeyword: String = InFile.keyword

  def taxIdentifierKeyword: String = TaxIdentifier.keyword

  def castableKeyword: String = Castable.keyword

  def validContainsKey(key: String): Boolean = ContainsDSL.isKeyDefined(key)

  def phoneNumberKeyword: String = PhoneNumber.keyword

  def socialSecurityNumberKeyword: String = SocialSecurityNumber.keyword

  def buildQuery(elements: String*): String = elements.mkString(" ")

  def buildOrQuery(elements: String*): String = elements.mkString(s" $orKeyword ")

  def in(listOfValues: Set[String]): FunctionDSL = In.noLevDSL(listOfValues)

  def inKeyword: String = In.keyword

  def rLike(regex: String): FunctionDSL = RLike.stringDSL(regex)

  def lowerKeyword: String = Lower.keyword

  def upperKeyword: String = Upper.keyword

  def replaceKeyword: String = Replace.keyword

  def andKeyword: String = AndOperator.keyword

  def trimKeyword: String = Trim.keyword

  def lengthKeyword: String = Length.keyword

  def columnKeyword: String = Column.keyword

  def rLikeKeyword: String = RLike.keyword

  def birthDate: FunctionDSL = BirthDate.noParamDSL

  def birthDateEnhanced: FunctionDSL = BirthDate.enhancedDSL

  def distributionKeyword: String = InlierNormalDist.keyword

  def contains(listOfValues: Seq[String]): FunctionDSL = Contains.containsDSL(listOfValues)

  def contains(listOfValues: Seq[String], caseSensitive: Boolean): FunctionDSL =
    Contains.containsDSL(listOfValues, caseSensitive)

  def distribution(func: FunctionDSL, mean: Double, std: Double): FunctionDSL =
    InlierNormalDist.distributionDSL(func, mean, std)

  def datumAndColumnFunction(datumFunction: FunctionDSL, columnFunction: FunctionDSL): FunctionDSL =
    datumFunction + separatorKeyword + columnFunction

  def colNameRLike(regex: String, mode: ColNameRLikeModeType = ColNameRLikeMode.sum): FunctionDSL =
    ColNameRLike.colNameRLike(regex, mode)

  def literal(value: Any): FunctionDSL = value.toString

  def literal(value: String): FunctionDSL = literal(s""""$value"""".asInstanceOf[Any])

  def between(func: FunctionDSL, min: Double, max: Double): FunctionDSL = Between.betweenDSL(func, min, max)

  def age: FunctionDSL = Age.ageDSL

  def orKeyword: String = OrOperator.keyword

  def colNameRLikeKeyword: String = ColNameRLike.keyword

  def notKeyword: String = NotOperator.keyword

  def accountNumberKeyword: String = AccountNumber.keyword

  def passportNumberKeyword: String = PassportNumber.keyword

  def buildAndQuery(elements: String*): String = elements.mkString(s" $andKeyword ")

  def socialUrl: FunctionDSL = SocialUrl.noParamDSL

  def socialUrlEnhanced: FunctionDSL = SocialUrl.enhancedDSL

  def accountNumber: FunctionDSL = AccountNumber.noParamDSL

  def accountNumberEnhanced: FunctionDSL = AccountNumber.enhancedDSL

  def taxIdentifier: FunctionDSL = TaxIdentifier.noParamDSL

  def taxIdentifierEnhanced: FunctionDSL = TaxIdentifier.enhancedDSL

  def phoneNumber: FunctionDSL = PhoneNumber.noParamDSL

  def phoneNumberEnhanced: FunctionDSL = PhoneNumber.enhancedDSL

  def passportNumber: FunctionDSL = PassportNumber.noParamDSL

  def passportNumberEnhanced: FunctionDSL = PassportNumber.enhancedDSL

  def socialSecurityNumber: FunctionDSL = SocialSecurityNumber.noParamDSL

  def socialSecurityNumber(countryCode: CountryCodeType): FunctionDSL = SocialSecurityNumber.countryCodeDSL(countryCode)

  def socialSecurityNumberEnhanced: FunctionDSL = SocialSecurityNumber.enhancedDSL

  def creditCard: FunctionDSL = CreditCard.noParamDSL

  def creditCardEnhanced: FunctionDSL = CreditCard.enhancedDSL

  def imei: FunctionDSL = IMEI.noParamDSL

  def imeiEnhanced: FunctionDSL = IMEI.enhancedDSL

  def residencePermitKeyword: String = ResidencePermit.keyword

  def residencePermit: FunctionDSL = ResidencePermit.noParamDSL

  def residencePermit(countryCode: CountryCodeType): FunctionDSL = ResidencePermit.countryCodeDSL(countryCode)

  def residencePermitEnhanced: FunctionDSL = ResidencePermit.enhancedDSL

  def identificationNumber: FunctionDSL = IdentificationNumber.noParamDSL

  def identificationNumber(countryCode: CountryCodeType): FunctionDSL = IdentificationNumber.countryCodeDSL(countryCode)

  def identificationNumberEnhanced: FunctionDSL = IdentificationNumber.enhancedDSL

  def identificationNumberKeyword: String = IdentificationNumber.keyword

  def ipAddress: FunctionDSL = IpAddress.noParamDSL

  def ipAddressEnhanced: FunctionDSL = IpAddress.enhancedDSL

  def drivingLicense: FunctionDSL = DrivingLicense.noParamDSL

  def drivingLicense(countryCode: CountryCodeType): FunctionDSL = DrivingLicense.countryCodeDSL(countryCode)

  def drivingLicenseEnhanced: FunctionDSL = DrivingLicense.enhancedDSL

  def drivingLicenseKeyword: String = DrivingLicense.keyword

  def geolocation: FunctionDSL = Geolocation.noParamDSL

  def geolocationEnhanced: FunctionDSL = Geolocation.enhancedDSL

  def documentKeyword: String = Document.keyword

  def document: FunctionDSL = Document.noParamDSL

  def document(someFileExtensions: Seq[FileExtensionType]): FunctionDSL = Document.fileExtensionsDSL(someFileExtensions)

  def documentEnhanced: FunctionDSL = Document.enhancedDSL

  def image: FunctionDSL = Image.noParamDSL

  def imageEnhanced: FunctionDSL = Image.enhancedDSL

  def video: FunctionDSL = Video.noParamDSL

  def videoEnhanced: FunctionDSL = Video.enhancedDSL

  def audio: FunctionDSL = Audio.noParamDSL

  def audioEnhanced: FunctionDSL = Audio.enhancedDSL

  def uuid: FunctionDSL = UUID.noParamDSL

  def uuidEnhanced: FunctionDSL = UUID.enhancedDSL

  def url: FunctionDSL = URL.noParamDSL

  def urlEnhanced: FunctionDSL = URL.enhancedDSL

  def uri: FunctionDSL = URI.noParamDSL

  def uriEnhanced: FunctionDSL = URI.enhancedDSL

  def email: FunctionDSL = Email.noParamDSL

  def personName: FunctionDSL = PersonName.noParamDSL

  def location: FunctionDSL = Location.noParamDSL

  def organization: FunctionDSL = Organization.noParamDSL

  def engineNumber: FunctionDSL = EngineNumber.noParamDSL

  def rewardPlan: FunctionDSL = RewardPlan.noParamDSL

  def occupationNLP: FunctionDSL = OccupationNLP.noParamDSL

  def username: FunctionDSL = Username.noParamDSL

  def surname: FunctionDSL = Surname.noParamDSL

  def name: FunctionDSL = Name.noParamDSL

  def booleanSet: FunctionDSL = BooleanSet.noParamDSL

  def date: FunctionDSL = Date.noParamDSL

  def timestamp: FunctionDSL = Timestamp.noParamDSL
}
