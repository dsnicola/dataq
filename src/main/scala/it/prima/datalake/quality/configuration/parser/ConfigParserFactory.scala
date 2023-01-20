package it.prima.datalake.quality.configuration.parser

object ConfigParserFactory {

  private final val YAML = "yaml"
  private final val YML = "yml"
  private final val JSON = "json"

  def createConfigParser(configPath: String): ConfigParser = {
    val extension = getFileExtension(configPath)
    extension match {
      case YAML | YML => new YAMLConfigParser
      case JSON => new JSONConfigParser
      case _ => throw new IllegalArgumentException(s"$extension not supported")
    }
  }

  def getFileExtension(file: String): String = {
    file.substring(file.lastIndexOf('.') + 1)
  }

}
