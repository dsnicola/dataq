package it.prima.datalake.quality.configuration

import it.prima.datalake.quality.configuration.parser.{ConfigParser, YAMLConfigParser}
import it.prima.datalake.quality.model.{DQConfig, DQConstraint, DQGroup}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class LocalConfigReaderTest extends AnyFunSuite with MockFactory {

  test("verify that ") {
    val path = "./src/test/resources/config.yaml"
    val configParser = mock[ConfigParser]
    val groups = List(
      DQGroup("Uniqueness", "All the checks to verify field uniqueness", "ERROR",
        List(DQConstraint(List("form_id"), "isUnique"))),
      DQGroup("Completeness", "All the checks to verify field completeness", "WARNING",
        List(DQConstraint(List("form_id", "is_report_uploaded"), "isComplete"))),
      DQGroup("Checks with comparison", "blab la", "WARNING",
        List(
          DQConstraint(List("phone_number"), "hasCompleteness", Some(">="), Some("0.56")),
          DQConstraint(List("phone_number"), "isContainedIn", value_list = Some(List("3334455666", "3400000000"))),
        )),
    )
    (configParser.parse _).expects(*).returns(DQConfig(groups = Some(groups)))

    val actual = new LocalConfigReader(path, configParser).readConfig()
    val expected = DQConfig(groups = Some(groups))

    assert(actual === expected)
  }

}
