package it.prima.datalake.quality.service

import it.prima.datalake.quality.model.{DQConfig, DQGroup}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{a, convertToAnyMustWrapper}

class QSStrategyFactoryTest extends AnyFunSuite {

  test("create complete strategy") {
    val dqConfig = DQConfig(Some(true), Some(List[DQGroup]()))
    val strategy = QSStrategyFactory.createStrategy(dqConfig, null)(null)

    strategy mustBe a[CompleteQualityService]
  }

  test("create analyze strategy") {
    val dqConfig = DQConfig(Some(true))
    val strategy = QSStrategyFactory.createStrategy(dqConfig, null) (null)

    strategy mustBe a[AnalyzeQualityService]
  }

  test("create check strategy") {
    val dqConfig = DQConfig(groups = Some(List[DQGroup]()))
    val strategy = QSStrategyFactory.createStrategy(dqConfig, null) (null)

    strategy mustBe a[CheckQualityService]
  }
}
