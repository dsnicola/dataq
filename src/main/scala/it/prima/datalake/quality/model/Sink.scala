package it.prima.datalake.quality.model

import it.prima.datalake.quality.constants.Format

case class Sink(checkPath: String,
                analysisPath: String,
                format: Format)
