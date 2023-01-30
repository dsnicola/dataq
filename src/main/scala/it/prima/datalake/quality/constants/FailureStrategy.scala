package it.prima.datalake.quality.constants

sealed trait FailureStrategy

case object FailFastStrategy extends FailureStrategy { val name = "FAIL_FAST"}
case object IgnoreMalformedStrategy extends FailureStrategy { val name = "IGNORE_MALFORMED"}