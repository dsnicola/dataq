package it.prima.datalake.quality.model

import it.prima.datalake.quality.constants.Format

case class Sink(path: String,
                corruptedPath: String,
                format: Format)
