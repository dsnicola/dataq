package it.prima.datalake.quality.configuration.parser

import io.circe.ParsingFailure
import it.prima.datalake.quality.model.{DQConfig, DQConstraint, DQGroup}
import org.scalatest.funsuite.AnyFunSuite

class YAMLConfigParserTest extends AnyFunSuite {

  test("parse") {
    val yamlConfigParser = new YAMLConfigParser

    val actual = yamlConfigParser.parse(
      """
        |groups:
        |  - name: "Uniqueness"
        |    description: "All the checks to verify field uniqueness"
        |    severity: "ERROR"
        |    checks:
        |      - columns:
        |        - "form_id"
        |        check_type: "isUnique"
        |  - name: "Completeness"
        |    description: "All the checks to verify field completeness"
        |    severity: "WARNING"
        |    checks:
        |      - columns:
        |          - "form_id"
        |          - "is_report_uploaded"
        |        check_type: "isComplete"
        |  - name: "Checks with comparison"
        |    description: "blab la"
        |    severity: "INFO"
        |    checks:
        |      - columns:
        |          - "phone_number"
        |        check_type: "hasCompleteness"
        |        operator: ">="
        |        value: "0.56"
        |      - columns:
        |          - "phone_number"
        |        check_type: "isContainedIn"
        |        value_list:
        |          - "3334455666"
        |          - "3400000000"
        |""".stripMargin
    )

    val groups = List(
      DQGroup("Uniqueness", "All the checks to verify field uniqueness", "ERROR",
        List(DQConstraint(List("form_id"), "isUnique"))),
      DQGroup("Completeness", "All the checks to verify field completeness", "WARNING",
        List(DQConstraint(List("form_id", "is_report_uploaded"), "isComplete"))),
      DQGroup("Checks with comparison", "blab la", "INFO",
        List(
          DQConstraint(List("phone_number"), "hasCompleteness", Some(">="), Some("0.56")),
          DQConstraint(List("phone_number"), "isContainedIn", value_list = Some(List("3334455666", "3400000000"))),
        )),

    )
    val expected = DQConfig(groups = Some(groups))

    assert(actual === expected)
  }

  test("parse analyze mode") {
    val yamlConfigParser = new YAMLConfigParser

    val actual = yamlConfigParser.parse(
      """
        |analyze: true
        |""".stripMargin
    )

    val expected = DQConfig(Some(true))

    assert(actual === expected)
  }

  test("malformed yaml") {
    val yamlConfigParser = new YAMLConfigParser
    assertThrows[ParsingFailure] {
      yamlConfigParser.parse(
        """
          |groups:
          | - name: "Uniqueness"
          |    description: "All the checks to verify field uniqueness"
          |    severity: "ERROR"
          |    checks:
          |      - columns:
          |        - "form_id"
          |        check_type: "isUnique"
          |  - name: "Completeness"
          |    description: "All the checks to verify field completeness"
          |    severity: "WARNING"
          |    checks:
          |      - columns:
          |          - "form_id"
          |          - "is_report_uploaded"
          |        check_type: "isComplete"
          |  - name: "Checks with comparison"
          |    description: "blab la"
          |    severity: "INFO"
          |    checks:
          |      - columns:
          |          - "phone_number"
          |        check_type: "hasCompleteness"
          |        operator: ">="
          |        value: "0.56"
          |      - columns:
          |          - "phone_number"
          |        check_type: "isContainedIn"
          |        value_list:
          |          - "3334455666"
          |          - "3400000000"
          |""".stripMargin
      )
    }
  }
}
