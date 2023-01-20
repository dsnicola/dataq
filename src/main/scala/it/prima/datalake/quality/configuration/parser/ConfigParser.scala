package it.prima.datalake.quality.configuration.parser

import it.prima.datalake.quality.model.DQConfig
import cats.syntax.either._
import io.circe._
import io.circe.generic.auto._
import io.circe.yaml.parser

trait ConfigParser {

  def parse(content: String): DQConfig = {
    val json = parser.parse(content)

    json
      .leftMap(err => err: Error)
      .flatMap(_.as[DQConfig])
      .valueOr(throw _)
  }

}
