package it.prima.datalake.quality.configuration.parser

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{a, convertToAnyMustWrapper}

class ConfigParserFactoryTest extends AnyFunSuite {

  test("getFileExtension") {
    val fileName = "a_file_name.yml"

    val actual = ConfigParserFactory.getFileExtension(fileName)
    val expected = "yml"

    assert(actual === expected)
  }

  test("create a YAML parser when file extension is yml") {
    val fileName = "a_file_name.yml"

    val actual = ConfigParserFactory.createConfigParser(fileName)

    actual mustBe a[YAMLConfigParser]
  }

  test("create a YAML parser when file extension is yaml") {
    val fileName = "a_file_name.yml"

    val actual = ConfigParserFactory.createConfigParser(fileName)

    actual mustBe a[YAMLConfigParser]
  }

  test("create a JSON parser when file extension is json") {
    val fileName = "a_file_name.json"

    val actual = ConfigParserFactory.createConfigParser(fileName)

    actual mustBe a[JSONConfigParser]
  }

  test("illegal file format") {
    val fileName = "a_file_name.txt"
    assertThrows[IllegalArgumentException] {
      ConfigParserFactory.createConfigParser(fileName)
    }
  }
}
