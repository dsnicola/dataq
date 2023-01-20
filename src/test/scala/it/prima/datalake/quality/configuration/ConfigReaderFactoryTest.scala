package it.prima.datalake.quality.configuration

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class ConfigReaderFactoryTest extends AnyFunSuite with Matchers {

  test("Create an S3 Config reader when a path with s3 is passed") {
    val path = "s3://some/path/file.yaml"
    val actual = ConfigReaderFactory.createConfigReader(path, null)

    actual mustBe a [S3ConfigReader]
  }

  test("Create an S3 Config reader when a path with s3a is passed") {
    val path = "s3a://some/path/file.yaml"
    val actual = ConfigReaderFactory.createConfigReader(path, null)

    actual mustBe a [S3ConfigReader]
  }

  test("Create a Local Config reader when a path with s3 is passed") {
    val path = "/some/path/file.yaml"
    val actual = ConfigReaderFactory.createConfigReader(path, null)

    actual mustBe a [LocalConfigReader]
  }
}
