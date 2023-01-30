package it.prima.datalake.quality.writer.failure.exceptions

import it.prima.datalake.quality.writer.failure.{FailFastStrategy, IgnoreMalformedStrategy}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{a, convertToAnyMustWrapper}

class FailureStrategyFactoryTest extends AnyFunSuite {

  test("Fail Fast strategy is being created") {
    val strategy = FailureStrategyFactory.createFailureStrategy("FAIL_FAST")

    strategy mustBe a[FailFastStrategy]
  }

  test("Ignore Malformed strategy is being created") {
    val strategy = FailureStrategyFactory.createFailureStrategy("IGNORE_MALFORMED")

    strategy mustBe a[IgnoreMalformedStrategy]
  }

  test("throws an error when the strategy does not exist") {
    assertThrows[IllegalArgumentException] {
      FailureStrategyFactory.createFailureStrategy("casual input")
    }
  }
}
