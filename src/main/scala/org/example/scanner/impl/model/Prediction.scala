package org.example.scanner.impl.model

object Prediction extends Enumeration {

  type PredictedEntity = Value
  val person: PredictedEntity = Value("PER")
  val organization: PredictedEntity = Value("ORG")
  val location: PredictedEntity = Value("LOC")
  val occupation: PredictedEntity = Value("OCCUPATION")
  val username: PredictedEntity = Value("USERNAME")
  val surname: PredictedEntity = Value("SURNAME")
  val name: PredictedEntity = Value("NAME")
  val engineNumber: PredictedEntity = Value("ENGINENUMBER")
  val rewardPlan: PredictedEntity = Value("REWARDPLAN")
  val nullEntity: PredictedEntity = Value("NONE")

  val PERSON_ENTITIES: Seq[PredictedEntity] = Seq(person)
  val LOCATION_ENTITIES: Seq[PredictedEntity] = Seq(organization, location)
  val ENGINE_NUMBER_ENTITIES: Seq[PredictedEntity] = Seq(engineNumber)
  val REWARDPLAN_ENTITIES: Seq[PredictedEntity] = Seq(rewardPlan)

  def findValue(s: String): Option[Value] = values.find(_.toString == s)

  def exists(s: String): Boolean = values.exists(_.toString == s)
}
