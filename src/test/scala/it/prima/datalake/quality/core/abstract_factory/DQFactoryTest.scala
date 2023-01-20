package it.prima.datalake.quality.core.abstract_factory

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{a, convertToAnyMustWrapper}

class DQFactoryTest extends AnyFunSuite {

  test("deequ factory creation") {
    val dqFactory = DQFactory.createFactory("DeequEngine")

    dqFactory mustBe a[DeequFactory]
  }

  test("engine not supported") {
    assertThrows[NoSuchElementException] {
      DQFactory.createFactory("GXEngine")
    }
  }
}
