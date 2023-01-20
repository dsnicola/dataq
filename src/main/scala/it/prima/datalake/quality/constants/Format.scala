package it.prima.datalake.quality.constants

sealed trait Format

object DELTA extends Format
object PARQUET extends Format
object CSV extends Format
